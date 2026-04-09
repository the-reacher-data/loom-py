"""Intermediate (temp) store for ETL pipeline checkpoints.

:class:`IntermediateStore` handles the physical read/write of intermediate
results materialised by :class:`~loom.etl.IntoTemp` targets and consumed by
:class:`~loom.etl.FromTemp` sources.

Physical formats
----------------
The backend is selected at construction time and injected via the *backend*
parameter:

* **Polars** :class:`polars.LazyFrame` — Arrow IPC written via ``sink_ipc()``
  (streaming, no in-memory collect) and read back via ``scan_ipc()`` (lazy,
  memory-mapped, predicate pushdown).
  Backend: :class:`~loom.etl.backends.polars._temp._PolarsTempBackend`.

* **Spark** ``pyspark.sql.DataFrame`` — Parquet directory written via
  ``df.write.parquet()`` and read via ``spark.read.parquet()``.  Cuts the
  lineage DAG; Photon-optimised.  Avoids competing with the shuffle memory
  pool that ``df.cache()`` would enter.
  Backend: :class:`~loom.etl.backends.spark._temp._SparkTempBackend`.

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
from typing import Any, Protocol, runtime_checkable

from loom.etl.storage.temp._cleaners import AutoTempCleaner, TempCleaner, _is_cloud_path
from loom.etl.storage.temp._scope import TempScope

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Backend Protocol — domain contract
# ---------------------------------------------------------------------------


@runtime_checkable
class _TempBackend(Protocol):
    """Physical I/O contract for one DataFrame backend.

    Both :class:`~loom.etl.backends.polars._temp._PolarsTempBackend` and
    :class:`~loom.etl.backends.spark._temp._SparkTempBackend` satisfy this
    protocol.  ``IntermediateStore`` holds a single ``_TempBackend`` chosen
    at construction time, keeping ``put`` / ``get`` free of backend branches.
    """

    def probe(self, name: str, base: str) -> Any | None:
        """Return the frame stored at (*name*, *base*), or ``None`` if absent."""
        ...

    def write(self, name: str, base: str, data: Any, *, append: bool) -> None:
        """Materialise *data* at (*name*, *base*)."""
        ...


# ---------------------------------------------------------------------------
# IntermediateStore
# ---------------------------------------------------------------------------


class IntermediateStore:
    """Physical store for intermediate ETL results.

    The backend (Polars or Spark) is injected at construction time via the
    *backend* parameter.  ``put`` and ``get`` are then backend-agnostic — no
    runtime type switching.

    Args:
        tmp_root: Root directory (local path or cloud URI) where
                  intermediates are stored.
        backend:  Physical I/O backend.  Defaults to
                  :class:`~loom.etl.backends.polars._temp._PolarsTempBackend`
                  with no cloud credentials — suitable for local paths.
        cleaner:  Strategy for deleting temp trees.  Defaults to
                  :class:`~loom.etl.AutoTempCleaner`.

    Example::

        from loom.etl.backends.polars._temp import _PolarsTempBackend

        tmp_root = os.environ["LOOM_TMP_ROOT"]
        store = IntermediateStore(
            tmp_root=tmp_root,
            backend=_PolarsTempBackend(storage_options={}),
        )
        store.put("orders", run_id="abc", correlation_id=None,
                  scope=TempScope.RUN, data=polars_lazy_frame)
        lf = store.get("orders", run_id="abc", correlation_id=None)
    """

    def __init__(
        self,
        tmp_root: str,
        backend: _TempBackend | None = None,
        cleaner: TempCleaner | None = None,
    ) -> None:
        self._root = tmp_root.rstrip("/")
        self._cleaner: TempCleaner = cleaner if cleaner is not None else AutoTempCleaner()
        self._backend: _TempBackend = backend if backend is not None else _default_polars_backend()

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
# Module-level helpers — shared path utilities
# ---------------------------------------------------------------------------


def _join_path(base: str, *parts: str) -> str:
    """Join *base* and *parts* into a single path, preserving cloud URI schemes."""
    root = base.rstrip("/")
    suffix = "/".join(part.strip("/") for part in parts if part)
    if not root:
        return f"/{suffix}" if suffix else "/"
    return f"{root}/{suffix}" if suffix else root


def _default_polars_backend() -> _TempBackend:
    """Return a Polars Arrow IPC backend with empty storage options.

    Imported lazily to avoid a mandatory ``polars`` import at module level —
    callers that only use Spark do not need Polars installed.
    """
    from loom.etl.backends.polars._temp import _PolarsTempBackend

    return _PolarsTempBackend(storage_options={})
