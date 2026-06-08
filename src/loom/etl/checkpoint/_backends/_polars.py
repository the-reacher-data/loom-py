"""Polars Arrow IPC backend for CheckpointStore."""

from __future__ import annotations

import logging
import threading
import uuid
from typing import Any, TypeGuard

import fsspec.core
import polars as pl

from loom.etl.checkpoint._cleaners import _checkpoint_storage_options, _join_path

_log = logging.getLogger(__name__)
_WRITING = ".writing"
_CLOUD_SCHEMES = ("s3://", "gs://", "gcs://", "abfss://", "abfs://", "az://")


def _is_polars_lazy_frame(value: Any) -> TypeGuard[pl.LazyFrame]:
    """Return True when value is a Polars LazyFrame."""
    return isinstance(value, pl.LazyFrame)


class _PolarsCheckpointBackend:
    """Polars Arrow IPC backend with atomic write-then-rename."""

    def __init__(self, storage_options: dict[str, str]) -> None:
        self._storage_options = _checkpoint_storage_options(storage_options)
        self._schemas: dict[str, pl.Schema] = {}
        self._lock = threading.Lock()

    def probe(self, name: str, base: str) -> Any | None:
        """Return LazyFrame scanning *name* under *base*, or None if absent."""
        scan_path = self._find_arrow(name, base)
        if scan_path is None:
            _log.info("checkpoint READ MISS name=%s base=%s", name, base)
            return None
        kwargs: dict[str, Any] = {"memory_map": not base.startswith(_CLOUD_SCHEMES)}
        polars_opts = _to_object_store_opts(self._storage_options)
        if polars_opts:
            kwargs["storage_options"] = polars_opts
        _log.info("checkpoint READ HIT  name=%s path=%s", name, scan_path)
        return pl.scan_ipc(scan_path, **kwargs)

    def write(self, name: str, base: str, data: Any, *, append: bool) -> None:
        """Sink *data* as Arrow IPC atomically (write-then-rename)."""
        if not _is_polars_lazy_frame(data):
            raise TypeError(
                f"Polars backend expects polars.LazyFrame, got {type(data).__qualname__!r}. "
                "Verify that CheckpointStore was constructed without a SparkSession."
            )

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

        _log.info("checkpoint WRITE     name=%s path=%s", name, final)
        _write_atomic(frame, tmp, final, self._storage_options)

    def _find_arrow(self, name: str, base: str) -> str | None:
        """Return scan path for *name* under *base*, or None if absent."""
        single = _arrow_path(name, base)
        fs, bp = fsspec.core.url_to_fs(base, **(self._storage_options or {}))
        if fs.exists(_join_path(bp, f"{name}.arrow")):
            return single
        dp = _join_path(bp, name)
        if fs.exists(dp) and fs.glob(_join_path(dp, "*.arrow")):
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


_FSSPEC_TO_OBJECT_STORE = {
    "endpoint_url": "aws_endpoint_url",
    "key": "aws_access_key_id",
    "secret": "aws_secret_access_key",  # NOSONAR: fsspec storage-option key name, not a credential
    "token": "aws_session_token",  # NOSONAR: fsspec storage-option key name, not a credential
}


def _to_object_store_opts(opts: dict[str, str] | None) -> dict[str, str]:
    """Translate fsspec-style storage options into Polars/object_store form.

    Polars' ``sink_ipc`` is backed by Rust ``object_store`` which expects
    ``aws_endpoint_url`` / ``aws_access_key_id`` / ``aws_secret_access_key``
    keys, NOT the fsspec ``endpoint_url`` / ``key`` / ``secret`` names.
    Without this translation Polars ignores the options and falls back to
    EC2 IMDS lookup — which fails inside non-AWS environments (MinIO, GCS
    via S3 API, on-prem) with a cryptic IMDS HTTP error.
    """
    if not opts:
        return {}
    out: dict[str, str] = {}
    for k, v in opts.items():
        out[_FSSPEC_TO_OBJECT_STORE.get(k, k)] = v
    return out


def _write_atomic(frame: pl.LazyFrame, tmp: str, final: str, opts: dict[str, str] | None) -> None:
    """Write to tmp, then rename to final atomically."""
    fs, p = fsspec.core.url_to_fs(tmp, **(opts or {}))
    fs.makedirs(fs._parent(p), exist_ok=True)
    polars_opts = _to_object_store_opts(opts)
    kwargs: dict[str, Any] = {"storage_options": polars_opts} if polars_opts else {}
    frame.sink_ipc(tmp, **kwargs)
    fs.rename(p, p.removesuffix(_WRITING))
