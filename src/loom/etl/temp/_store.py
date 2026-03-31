"""Intermediate (temp) store for ETL pipeline checkpoints.

:class:`IntermediateStore` handles the physical read/write of intermediate
results materialised by :class:`~loom.etl.IntoTemp` targets and consumed by
:class:`~loom.etl.FromTemp` sources.

Physical formats
----------------
The backend is selected at construction time based on whether a
``SparkSession`` is provided:

* **Polars** :class:`polars.LazyFrame` — Arrow IPC written via ``sink_ipc()``
  (streaming, no in-memory collect) and read back via ``scan_ipc()`` (lazy,
  memory-mapped, predicate pushdown).

* **Spark** ``pyspark.sql.DataFrame`` — Parquet directory written via
  ``df.write.parquet()`` and read via ``spark.read.parquet()``.  Cuts the
  lineage DAG; Photon-optimised.  Avoids competing with the shuffle memory
  pool that ``df.cache()`` would enter.

Path structure
--------------
RUN scope::

    {tmp_root}/runs/{run_id}/{name}.arrow   ← Polars
    {tmp_root}/runs/{run_id}/{name}/        ← Spark (Parquet dir)

CORRELATION scope::

    {tmp_root}/correlations/{correlation_id}/{name}.arrow
    {tmp_root}/correlations/{correlation_id}/{name}/

Cleanup
-------
* :meth:`cleanup_run` — removes the entire ``runs/{run_id}/`` tree.
  Called in the ``finally`` of every pipeline run.

* :meth:`cleanup_correlation` — removes ``correlations/{correlation_id}/``.
  Called by :class:`~loom.etl.ETLRunner` on successful last attempt, or
  manually via :meth:`~loom.etl.ETLRunner.cleanup_correlation`.

* :meth:`cleanup_stale` — removes run directories older than a threshold.
  Useful for garbage-collecting orphaned runs after a crash.
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from typing import Any, Protocol, runtime_checkable

import fsspec.core
import polars as pl

from loom.etl.temp._cleaners import AutoTempCleaner, TempCleaner, _is_cloud_path
from loom.etl.temp._scope import TempScope

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Backend Protocol + implementations
# ---------------------------------------------------------------------------


@runtime_checkable
class _TempBackend(Protocol):
    """Physical I/O contract for one DataFrame backend.

    Both :class:`_PolarsTempBackend` and :class:`_SparkTempBackend` satisfy
    this protocol.  ``IntermediateStore`` holds a single ``_TempBackend`` chosen
    at construction time, keeping ``put`` / ``get`` free of backend branches.
    """

    def probe(self, name: str, base: str) -> Any | None:
        """Return the frame stored at (*name*, *base*), or ``None`` if absent."""
        ...

    def write(self, name: str, base: str, data: Any, *, append: bool) -> None:
        """Materialise *data* at (*name*, *base*)."""
        ...


class _PolarsTempBackend:
    """Polars Arrow IPC backend for :class:`IntermediateStore`."""

    def __init__(self, storage_options: dict[str, str]) -> None:
        self._storage_options = storage_options

    def probe(self, name: str, base: str) -> Any | None:
        """Return a ``LazyFrame`` scanning *name* under *base*, or ``None`` if absent."""
        scan_path = self._find_arrow(name, base)
        if scan_path is None:
            return None
        cloud = _is_cloud_path(base)
        kwargs: dict[str, Any] = {"memory_map": not cloud}
        if self._storage_options:
            kwargs["storage_options"] = self._storage_options
        return pl.scan_ipc(scan_path, **kwargs)

    def write(self, name: str, base: str, data: Any, *, append: bool) -> None:
        """Sink *data* as Arrow IPC under *base*.

        Raises:
            TypeError: When *data* is not a ``polars.LazyFrame``.
        """
        if not _is_polars_lazy_frame(data):
            raise TypeError(
                f"Polars backend expects polars.LazyFrame, got {type(data).__qualname__!r}. "
                "Verify that IntermediateStore was constructed without a SparkSession."
            )
        path = _arrow_part(name, base) if append else _arrow_path(name, base)
        _log.debug("temp write arrow path=%s append=%s", path, append)
        _ensure_parent(path)
        kwargs: dict[str, Any] = {}
        if self._storage_options:
            kwargs["storage_options"] = self._storage_options
        data.sink_ipc(path, **kwargs)

    def _find_arrow(self, name: str, base: str) -> str | None:
        """Return the scan path for *name* under *base*, or ``None`` if absent.

        For cloud URIs uses ``fsspec`` to check existence — ``os.path`` only
        works on local filesystems.  For local paths falls back to the standard
        ``os.path.exists`` / ``os.path.isdir`` checks.
        """
        single = _arrow_path(name, base)
        directory = _join_path(base, name)
        if _is_cloud_path(base):
            fs, base_path = fsspec.core.url_to_fs(base, **(self._storage_options or {}))
            if fs.exists(_join_path(base_path, f"{name}.arrow")):
                return single
            if fs.isdir(_join_path(base_path, name)):
                return _join_path(directory, "*.arrow")
            return None
        if os.path.exists(single):
            return single
        if os.path.isdir(directory):
            return _join_path(directory, "*.arrow")
        return None


class _SparkTempBackend:
    """PySpark Parquet backend for :class:`IntermediateStore`."""

    def __init__(self, spark: Any) -> None:
        self._spark = spark

    def probe(self, name: str, base: str) -> Any | None:
        """Return a ``DataFrame`` reading *name* under *base*, or ``None`` if absent."""
        return _probe_spark(self._spark, _join_path(base, name))

    def write(self, name: str, base: str, data: Any, *, append: bool) -> None:
        """Write *data* as a Parquet directory under *base*.

        Raises:
            TypeError: When *data* is not a ``pyspark.sql.DataFrame``.
        """
        if not _is_spark_dataframe(data):
            raise TypeError(
                f"Spark backend expects pyspark.sql.DataFrame, got {type(data).__qualname__!r}. "
                "Verify that IntermediateStore was constructed with a SparkSession."
            )
        path = _join_path(base, name)
        _log.debug("temp write parquet path=%s append=%s", path, append)
        data.write.mode("append" if append else "overwrite").parquet(path)


# ---------------------------------------------------------------------------
# IntermediateStore
# ---------------------------------------------------------------------------


class IntermediateStore:
    """Physical store for intermediate ETL results.

    The backend (Polars or Spark) is selected once at construction time based
    on whether a ``SparkSession`` is provided.  ``put`` and ``get`` are then
    backend-agnostic — no runtime type switching.

    Args:
        tmp_root:        Root directory (local path or cloud URI) where
                         intermediates are stored.
        storage_options: Cloud credentials forwarded to ``polars.sink_ipc``
                         when using a cloud URI.  Ignored for Spark (the
                         ``SparkSession`` handles credentials).
        spark:           Active :class:`pyspark.sql.SparkSession`.  When
                         provided, selects the Spark backend.
        cleaner:         Strategy for deleting temp trees.  Defaults to
                         :class:`~loom.etl.AutoTempCleaner`.

    Example::

        tmp_root = os.environ["LOOM_TMP_ROOT"]
        store = IntermediateStore(tmp_root=tmp_root, storage_options={})
        store.put("orders", run_id="abc", correlation_id=None,
                  scope=TempScope.RUN, data=polars_lazy_frame)
        lf = store.get("orders", run_id="abc", correlation_id=None)
    """

    def __init__(
        self,
        tmp_root: str,
        storage_options: dict[str, str] | None = None,
        spark: Any = None,
        cleaner: TempCleaner | None = None,
    ) -> None:
        self._root = tmp_root.rstrip("/")
        self._cleaner: TempCleaner = cleaner if cleaner is not None else AutoTempCleaner()
        self._backend: _TempBackend = (
            _SparkTempBackend(spark)
            if spark is not None
            else _PolarsTempBackend(storage_options or {})
        )

    # ------------------------------------------------------------------
    # Public write / read
    # ------------------------------------------------------------------

    def put(
        self,
        name: str,
        *,
        run_id: str,
        correlation_id: str | None,
        scope: TempScope,
        data: Any,
        append: bool = False,
    ) -> None:
        """Materialise *data* as a named intermediate.

        Args:
            name:           Logical name matching :class:`~loom.etl.IntoTemp`.
            run_id:         UUID of the current pipeline run.
            correlation_id: Logical job ID grouping retries.  Required when
                            *scope* is :attr:`~TempScope.CORRELATION`.
            scope:          Whether to keep across retries.
            data:           ``polars.LazyFrame`` or ``pyspark.sql.DataFrame``.
            append:         When ``True``, writes a new partition file alongside
                            any previously written parts for this name (fan-in).
                            When ``False`` (default), the write is exclusive —
                            any prior content for this name is overwritten.

        Raises:
            ValueError: When *scope* is ``CORRELATION`` and *correlation_id*
                        is ``None``.
            TypeError:  When *data* type does not match the configured backend.
        """
        if scope is TempScope.CORRELATION and not correlation_id:
            raise ValueError(
                f"IntoTemp({name!r}, scope=CORRELATION) requires a correlation_id. "
                "Pass correlation_id= to ETLRunner.run()."
            )
        _log.debug("temp put name=%r scope=%s append=%s", name, scope, append)
        base = self._scope_base(run_id=run_id, correlation_id=correlation_id, scope=scope)
        self._backend.write(name, base, data, append=append)

    def get(
        self,
        name: str,
        *,
        run_id: str,
        correlation_id: str | None,
    ) -> Any:
        """Load a previously materialised intermediate.

        Tries the RUN-scope path first, then falls back to the CORRELATION-scope
        path.  This allows :class:`~loom.etl.FromTemp` consumers to be
        scope-agnostic — they only declare the name.

        Returns a :class:`polars.LazyFrame` for Polars intermediates, or a
        ``pyspark.sql.DataFrame`` for Spark intermediates.

        Args:
            name:           Logical name matching :class:`~loom.etl.FromTemp`.
            run_id:         UUID of the current pipeline run.
            correlation_id: Logical job ID.  Checked when RUN path is absent.

        Raises:
            FileNotFoundError: When no intermediate exists at either path.
        """
        run_base = self._scope_base(
            run_id=run_id, correlation_id=correlation_id, scope=TempScope.RUN
        )
        result = self._backend.probe(name, run_base)
        if result is not None:
            _log.debug("temp get name=%r scope=run", name)
            return result
        if correlation_id is not None:
            corr_base = self._scope_base(
                run_id=run_id, correlation_id=correlation_id, scope=TempScope.CORRELATION
            )
            result = self._backend.probe(name, corr_base)
            if result is not None:
                _log.debug("temp get name=%r scope=correlation", name)
                return result
        raise FileNotFoundError(
            f"Intermediate {name!r} not found. "
            f"Check that IntoTemp({name!r}) ran before FromTemp({name!r})."
        )

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def cleanup_run(self, run_id: str) -> None:
        """Remove all RUN-scope intermediates for *run_id*.

        Safe to call even when no intermediates were written.

        Args:
            run_id: UUID of the pipeline run to clean.
        """
        path = _join_path(self._root, "runs", run_id)
        _log.debug("temp cleanup run path=%s", path)
        self._cleaner.delete_tree(path)

    def cleanup_correlation(self, correlation_id: str) -> None:
        """Remove all CORRELATION-scope intermediates for *correlation_id*.

        Call this after the final successful attempt, or to reclaim space
        after a permanently failed job.

        Args:
            correlation_id: Logical job ID whose intermediates to purge.
        """
        path = _join_path(self._root, "correlations", correlation_id)
        _log.debug("temp cleanup correlation path=%s", path)
        self._cleaner.delete_tree(path)

    def cleanup_stale(self, *, older_than_seconds: int = 86_400) -> None:
        """Remove run directories not modified within *older_than_seconds*.

        Only supported for local ``tmp_root`` paths — cloud storage does not
        expose directory modification times via the OS.  For cloud environments
        configure a bucket lifecycle / retention policy on ``tmp_root`` instead.

        Args:
            older_than_seconds: Age threshold in seconds.  Defaults to 86 400
                                (24 hours).
        """
        runs_root = _join_path(self._root, "runs")
        if _is_cloud_path(runs_root):
            _log.warning(
                "cleanup_stale not supported for cloud tmp_root=%s — "
                "configure a bucket lifecycle policy instead.",
                self._root,
            )
            return
        if not os.path.isdir(runs_root):
            return
        cutoff = time.time() - older_than_seconds
        removed = 0
        for entry in os.scandir(runs_root):
            if entry.is_dir() and entry.stat().st_mtime < cutoff:
                _log.debug("temp cleanup stale dir=%s", entry.path)
                self._cleaner.delete_tree(entry.path)
                removed += 1
        if removed:
            _log.info(
                "temp cleanup stale removed=%d older_than_seconds=%d", removed, older_than_seconds
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _scope_base(
        self,
        *,
        run_id: str,
        correlation_id: str | None,
        scope: TempScope,
    ) -> str:
        if scope is TempScope.CORRELATION:
            if correlation_id is None:
                raise RuntimeError(
                    "_scope_base called with scope=CORRELATION but correlation_id=None. "
                    "This is a framework bug — please report it."
                )
            return _join_path(self._root, "correlations", correlation_id)
        return _join_path(self._root, "runs", run_id)


# ---------------------------------------------------------------------------
# Module-level helpers — no state
# ---------------------------------------------------------------------------


def _is_polars_lazy_frame(obj: Any) -> bool:
    t = type(obj)
    return t.__module__.startswith("polars") and t.__name__ == "LazyFrame"


def _is_spark_dataframe(obj: Any) -> bool:
    t = type(obj)
    return "pyspark" in t.__module__ and t.__name__ == "DataFrame"


def _arrow_path(name: str, base: str) -> str:
    """Single-file Arrow IPC path for strict (non-append) writes."""
    return _join_path(base, f"{name}.arrow")


def _arrow_part(name: str, base: str) -> str:
    """Unique partition path inside the append directory for *name*."""
    return _join_path(base, name, f"{uuid.uuid4().hex}.arrow")


def _probe_spark(spark: Any, path: str) -> Any | None:
    """Read a Parquet path via *spark*; return ``None`` when the path does not exist.

    Catches ``pyspark.sql.utils.AnalysisException`` only — the specific Spark
    exception for missing paths and unresolvable table references.  Checked by
    module prefix and class name to avoid importing pyspark at module level
    (optional dependency).  Any other exception is re-raised.
    """
    try:
        return spark.read.parquet(path)
    except Exception as exc:
        t = type(exc)
        if t.__module__.startswith("pyspark") and t.__name__ == "AnalysisException":
            return None
        raise


def _ensure_parent(path: str) -> None:
    if _is_cloud_path(path):
        return  # object stores create key prefixes implicitly on write
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def _join_path(base: str, *parts: str) -> str:
    root = base.rstrip("/")
    suffix = "/".join(part.strip("/") for part in parts if part)
    if not root:
        return f"/{suffix}" if suffix else "/"
    return f"{root}/{suffix}" if suffix else root
