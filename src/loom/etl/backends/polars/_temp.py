"""Polars Arrow IPC backend for IntermediateStore.

Internal module — not exported from the Polars backend package.
"""

from __future__ import annotations

import logging
import os
import uuid
from typing import Any

import fsspec.core
import polars as pl

from loom.etl.temp._cleaners import _is_cloud_path
from loom.etl.temp._store import _join_path

_log = logging.getLogger(__name__)


class _PolarsTempBackend:
    """Polars Arrow IPC backend for :class:`~loom.etl.temp._store.IntermediateStore`.

    Writes Polars :class:`polars.LazyFrame` instances as Arrow IPC files via
    ``sink_ipc()`` (streaming, no in-memory collect) and reads them back via
    ``scan_ipc()`` (lazy, memory-mapped, predicate pushdown).

    Args:
        storage_options: Cloud credentials forwarded to ``polars.sink_ipc``
                         when using a cloud URI.  Pass an empty dict for local
                         storage or when credentials come from the environment.
    """

    def __init__(self, storage_options: dict[str, str]) -> None:
        self._storage_options = storage_options

    def probe(self, name: str, base: str) -> Any | None:
        """Return a ``LazyFrame`` scanning *name* under *base*, or ``None`` if absent.

        Args:
            name: Logical intermediate name.
            base: Scope-level base directory path.

        Returns:
            Lazy :class:`polars.LazyFrame` when found; ``None`` otherwise.
        """
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

        Args:
            name:   Logical intermediate name.
            base:   Scope-level base directory path.
            data:   :class:`polars.LazyFrame` to materialise.
            append: When ``True``, writes a new partition file alongside any
                    previously written parts for this name (fan-in).  When
                    ``False``, performs an exclusive write.

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


# ---------------------------------------------------------------------------
# Module-level helpers — no state, only used by _PolarsTempBackend
# ---------------------------------------------------------------------------


def _is_polars_lazy_frame(obj: Any) -> bool:
    """Return ``True`` when *obj* is a ``polars.LazyFrame``."""
    t = type(obj)
    return t.__module__.startswith("polars") and t.__name__ == "LazyFrame"


def _arrow_path(name: str, base: str) -> str:
    """Single-file Arrow IPC path for strict (non-append) writes."""
    return _join_path(base, f"{name}.arrow")


def _arrow_part(name: str, base: str) -> str:
    """Unique partition path inside the append directory for *name*."""
    return _join_path(base, name, f"{uuid.uuid4().hex}.arrow")


def _ensure_parent(path: str) -> None:
    """Create parent directories for *path* when using a local filesystem."""
    if _is_cloud_path(path):
        return  # object stores create key prefixes implicitly on write
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
