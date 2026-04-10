"""Polars Arrow IPC backend for CheckpointStore."""

from __future__ import annotations

import logging
import os
import threading
import uuid
from typing import Any

import fsspec.core
import polars as pl

from loom.etl.checkpoint._cleaners import _is_cloud_path, _join_path

_log = logging.getLogger(__name__)
_WRITING = ".writing"


class _PolarsCheckpointBackend:
    """Polars Arrow IPC backend with atomic write-then-rename."""

    def __init__(self, storage_options: dict[str, str]) -> None:
        self._storage_options = storage_options
        self._schemas: dict[str, pl.Schema] = {}
        self._lock = threading.Lock()

    def probe(self, name: str, base: str) -> Any | None:
        """Return LazyFrame scanning *name* under *base*, or None if absent."""
        scan_path = self._find_arrow(name, base)
        if scan_path is None:
            return None
        kwargs: dict[str, Any] = {"memory_map": not _is_cloud_path(base)}
        if self._storage_options:
            kwargs["storage_options"] = self._storage_options
        return pl.scan_ipc(scan_path, **kwargs)

    def write(self, name: str, base: str, data: Any, *, append: bool) -> None:
        """Sink *data* as Arrow IPC atomically (write-then-rename)."""
        if not isinstance(data, pl.LazyFrame):
            raise TypeError(f"Expected polars.LazyFrame, got {type(data).__qualname__!r}")

        frame = data
        key = f"{base}/{name}"

        with self._lock:
            existing = self._schemas.get(key)
            if existing is None:
                self._schemas[key] = frame.collect_schema()

        if append and existing is not None:
            frame = _align_lazy_to_schema(frame, existing)

        final = _arrow_part(name, base) if append else _arrow_path(name, base)
        tmp = f"{final}{_WRITING}"

        _log.debug("checkpoint write tmp=%s final=%s", tmp, final)
        _write_atomic(frame, tmp, final, self._storage_options)

    def _find_arrow(self, name: str, base: str) -> str | None:
        """Return scan path for *name* under *base*, or None if absent."""
        single = _arrow_path(name, base)
        if _is_cloud_path(base):
            fs, bp = fsspec.core.url_to_fs(base, **(self._storage_options or {}))
            if fs.exists(_join_path(bp, f"{name}.arrow")):
                return single
            dp = _join_path(bp, name)
            if fs.exists(dp) and fs.glob(_join_path(dp, "*.arrow")):
                return _join_path(base, name, "*.arrow")
            return None
        if os.path.exists(single):
            return single
        dp = os.path.join(base, name)
        if os.path.isdir(dp) and any(f.endswith(".arrow") for f in os.listdir(dp)):
            return _join_path(base, name, "*.arrow")
        return None


def _arrow_path(name: str, base: str) -> str:
    return _join_path(base, f"{name}.arrow")


def _arrow_part(name: str, base: str) -> str:
    return _join_path(base, name, f"{uuid.uuid4().hex}.arrow")


def _align_lazy_to_schema(frame: pl.LazyFrame, schema: pl.Schema) -> pl.LazyFrame:
    source = frame.collect_schema()
    exprs: list[pl.Expr] = []
    for n, dt in schema.items():
        exprs.append(
            pl.col(n).cast(dt, strict=False).alias(n)
            if n in source.names()
            else pl.lit(None).cast(dt).alias(n)
        )
    return frame.select(exprs)


def _write_atomic(frame: pl.LazyFrame, tmp: str, final: str, opts: dict[str, str] | None) -> None:
    """Write to tmp, then rename to final atomically."""
    if not _is_cloud_path(tmp):
        os.makedirs(os.path.dirname(tmp), exist_ok=True)
    kwargs = {"storage_options": opts} if opts else {}
    frame.sink_ipc(tmp, **kwargs)
    if _is_cloud_path(tmp):
        fs, p = fsspec.core.url_to_fs(tmp, **(opts or {}))
        fs.rename(p, p.replace(_WRITING, ""))
    else:
        os.rename(tmp, final)
