"""Intermediate (temp) store for ETL pipeline checkpoints.

:class:`IntermediateStore` handles the physical read/write of intermediate
results materialised by :class:`~loom.etl.IntoTemp` targets and consumed by
:class:`~loom.etl.FromTemp` sources.

Physical formats
----------------
The backend is detected from the concrete DataFrame type at ``put()`` time:

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
from typing import Any

from loom.etl.temp._cleaners import AutoTempCleaner, TempCleaner, _is_cloud_path
from loom.etl.temp._scope import TempScope

_log = logging.getLogger(__name__)


class IntermediateStore:
    """Physical store for intermediate ETL results.

    Handles Polars (Arrow IPC) and Spark (Parquet) backends transparently.
    The backend is auto-detected from the DataFrame type at :meth:`put` time.

    Args:
        tmp_root:        Root directory (local path or cloud URI) where
                         intermediates are stored.
        storage_options: Cloud credentials for the underlying storage, forwarded
                         to ``polars.sink_ipc`` when using a cloud URI.
                         Ignored for Spark (SparkSession handles credentials).
        spark:           Active :class:`pyspark.sql.SparkSession`.  Required
                         when writing/reading Spark DataFrames.

    Example::

        store = IntermediateStore(tmp_root="/tmp/loom", storage_options={})
        store.put("orders", run_id="abc", correlation_id=None,
                  scope=TempScope.RUN, data=polars_lazy_frame)
        lf = store.get("orders", run_id="abc", correlation_id=None,
                       scope=TempScope.RUN)
    """

    def __init__(
        self,
        tmp_root: str,
        storage_options: dict[str, str] | None = None,
        spark: Any = None,
        cleaner: TempCleaner | None = None,
    ) -> None:
        self._root = tmp_root.rstrip("/")
        self._storage_options: dict[str, str] = storage_options or {}
        self._spark = spark
        self._cleaner: TempCleaner = cleaner if cleaner is not None else AutoTempCleaner()

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
            TypeError: When *data* is neither a Polars LazyFrame nor a Spark
                       DataFrame.
            ValueError: When *scope* is ``CORRELATION`` and *correlation_id*
                        is ``None``.
        """
        if scope is TempScope.CORRELATION and not correlation_id:
            raise ValueError(
                f"IntoTemp({name!r}, scope=CORRELATION) requires a correlation_id. "
                "Pass correlation_id= to ETLRunner.run()."
            )
        if _is_polars_lazy_frame(data):
            _log.debug("temp put name=%r scope=%s backend=polars append=%s", name, scope, append)
            self._put_polars(
                name,
                run_id=run_id,
                correlation_id=correlation_id,
                scope=scope,
                lf=data,
                append=append,
            )
        elif _is_spark_dataframe(data):
            _log.debug("temp put name=%r scope=%s backend=spark append=%s", name, scope, append)
            self._put_spark(
                name,
                run_id=run_id,
                correlation_id=correlation_id,
                scope=scope,
                df=data,
                append=append,
            )
        else:
            raise TypeError(
                f"IntermediateStore.put(): unsupported DataFrame type {type(data).__qualname__!r}. "
                "Expected polars.LazyFrame or pyspark.sql.DataFrame."
            )

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
        if self._spark is not None:
            return self._get_spark(name, run_id=run_id, correlation_id=correlation_id)
        return self._get_polars(name, run_id=run_id, correlation_id=correlation_id)

    def _get_polars(
        self,
        name: str,
        *,
        run_id: str,
        correlation_id: str | None,
    ) -> Any:
        import polars as pl

        run_base = self._scope_base(
            run_id=run_id, correlation_id=correlation_id, scope=TempScope.RUN
        )
        if scan_path := _find_arrow(name, run_base):
            _log.debug("temp get name=%r resolved=run path=%s", name, scan_path)
            return pl.scan_ipc(scan_path, memory_map=True)
        if correlation_id is not None:
            corr_base = self._scope_base(
                run_id=run_id, correlation_id=correlation_id, scope=TempScope.CORRELATION
            )
            if scan_path := _find_arrow(name, corr_base):
                _log.debug("temp get name=%r resolved=correlation path=%s", name, scan_path)
                return pl.scan_ipc(scan_path, memory_map=True)
        raise FileNotFoundError(
            f"Intermediate {name!r} not found. "
            f"Check that IntoTemp({name!r}) ran before FromTemp({name!r})."
        )

    def _get_spark(
        self,
        name: str,
        *,
        run_id: str,
        correlation_id: str | None,
    ) -> Any:
        run_path = self._spark_path(
            name, run_id=run_id, correlation_id=correlation_id, scope=TempScope.RUN
        )
        df = _probe_spark(self._spark, run_path)
        if df is not None:
            return df
        if correlation_id is not None:
            corr_path = self._spark_path(
                name, run_id=run_id, correlation_id=correlation_id, scope=TempScope.CORRELATION
            )
            df = _probe_spark(self._spark, corr_path)
            if df is not None:
                return df
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
        path = f"{self._root}/runs/{run_id}"
        _log.debug("temp cleanup run path=%s", path)
        self._cleaner.delete_tree(path)

    def cleanup_correlation(self, correlation_id: str) -> None:
        """Remove all CORRELATION-scope intermediates for *correlation_id*.

        Call this after the final successful attempt, or to reclaim space
        after a permanently failed job.

        Args:
            correlation_id: Logical job ID whose intermediates to purge.
        """
        path = f"{self._root}/correlations/{correlation_id}"
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
        runs_root = f"{self._root}/runs"
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

    def _spark_path(
        self,
        name: str,
        *,
        run_id: str,
        correlation_id: str | None,
        scope: TempScope,
    ) -> str:
        base = self._scope_base(run_id=run_id, correlation_id=correlation_id, scope=scope)
        return f"{base}/{name}"

    def _scope_base(
        self,
        *,
        run_id: str,
        correlation_id: str | None,
        scope: TempScope,
    ) -> str:
        if scope is TempScope.CORRELATION:
            return f"{self._root}/correlations/{correlation_id}"
        return f"{self._root}/runs/{run_id}"

    def _put_polars(
        self,
        name: str,
        *,
        run_id: str,
        correlation_id: str | None,
        scope: TempScope,
        lf: Any,
        append: bool,
    ) -> None:
        base = self._scope_base(run_id=run_id, correlation_id=correlation_id, scope=scope)
        path = _arrow_part(name, base) if append else _arrow_path(name, base)
        _log.debug("temp write arrow path=%s append=%s", path, append)
        _ensure_parent(path)
        kwargs: dict[str, Any] = {}
        if self._storage_options:
            kwargs["storage_options"] = self._storage_options
        lf.sink_ipc(path, **kwargs)

    def _put_spark(
        self,
        name: str,
        *,
        run_id: str,
        correlation_id: str | None,
        scope: TempScope,
        df: Any,
        append: bool,
    ) -> None:
        path = self._spark_path(name, run_id=run_id, correlation_id=correlation_id, scope=scope)
        _log.debug("temp write parquet path=%s append=%s", path, append)
        df.write.mode("append" if append else "overwrite").parquet(path)


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
    return f"{base}/{name}.arrow"


def _arrow_part(name: str, base: str) -> str:
    """Unique partition path inside the append directory for *name*."""
    return f"{base}/{name}/{uuid.uuid4().hex}.arrow"


def _find_arrow(name: str, base: str) -> str | None:
    """Return the scan path for *name* under *base*, or ``None`` if absent.

    Checks the single-file path first (strict write), then the directory
    (append fan-in).  Returns a glob pattern for directories so that
    ``polars.scan_ipc`` picks up all parts in one call.
    """
    single = _arrow_path(name, base)
    if os.path.exists(single):
        return single
    directory = f"{base}/{name}"
    if os.path.isdir(directory):
        return f"{directory}/*.arrow"
    return None


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
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
