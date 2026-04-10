"""Checkpoint cleaners."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Protocol, runtime_checkable

import fsspec.core

_log = logging.getLogger(__name__)


_CLOUD_SCHEMES = frozenset({"s3", "gs", "gcs", "abfss", "abfs", "az", "r2"})


def _is_cloud_path(path: str) -> bool:
    """Return ``True`` when *path* uses a supported cloud URI scheme."""
    if "://" not in path:
        return False
    scheme, _ = path.split("://", 1)
    return scheme.lower() in _CLOUD_SCHEMES


@runtime_checkable
class TempCleaner(Protocol):
    """Protocol for deleting checkpoint trees."""

    def delete_tree(self, path: str) -> None:
        """Delete *path* recursively."""
        ...


class FsspecTempCleaner:
    """Delete checkpoint trees via fsspec (s3, gs, abfss, etc)."""

    def __init__(self, storage_options: Mapping[str, str] | None = None) -> None:
        self._storage_options = dict(storage_options or {})

    def delete_tree(self, path: str) -> None:
        """Remove *path* and its contents from cloud storage.

        Failure is non-fatal — a ``WARNING`` is logged instead.
        """
        try:
            fs, fpath = fsspec.core.url_to_fs(path, **self._storage_options)
            if fs.exists(fpath):
                _log.debug("checkpoint cleanup path=%s", path)
                fs.rm(fpath, recursive=True)
        except Exception as exc:
            _log.warning("checkpoint cleanup skipped path=%s reason=%s", path, exc)


# Backward-compatible alias for public imports.
CheckpointCleaner = FsspecTempCleaner


def _join_path(base: str, *parts: str) -> str:
    """Join *base* and *parts* into a single path, preserving cloud URI schemes."""
    root = base.rstrip("/")
    suffix = "/".join(part.strip("/") for part in parts if part)
    if not root:
        return f"/{suffix}" if suffix else "/"
    return f"{root}/{suffix}" if suffix else root
