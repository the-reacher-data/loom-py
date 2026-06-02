"""Checkpoint cleaners."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable

import fsspec.core

_log = logging.getLogger(__name__)


class CheckpointCleanupError(RuntimeError):
    """Raised when a checkpoint tree cannot be deleted."""


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
        self._storage_options = _checkpoint_storage_options(dict(storage_options or {}))

    def delete_tree(self, path: str) -> None:
        """Remove *path* and its contents from cloud storage.

        Raises:
            CheckpointCleanupError: When the deletion fails for any reason.
        """
        try:
            fs, fpath = fsspec.core.url_to_fs(path, **self._storage_options)
            if fs.exists(fpath):
                _log.debug("checkpoint cleanup path=%s", path)
                fs.rm(fpath, recursive=True)
        except Exception as exc:
            _log.warning("checkpoint cleanup skipped path=%r reason=%s", path, exc)


# Backward-compatible alias for public imports.
CheckpointCleaner = FsspecTempCleaner


def _join_path(base: str, *parts: str) -> str:
    """Join *base* and *parts* into a single path, preserving cloud URI schemes."""
    root = base.rstrip("/")
    suffix = "/".join(part.strip("/") for part in parts if part)
    if not root:
        return f"/{suffix}" if suffix else "/"
    return f"{root}/{suffix}" if suffix else root


def _checkpoint_storage_options(storage_options: Mapping[str, str]) -> dict[str, Any]:
    """Return fsspec-compatible options for checkpoint cleanup/write paths."""
    endpoint = storage_options.get("endpoint_url") or storage_options.get("AWS_ENDPOINT_URL", "")
    if not endpoint:
        return {}

    options: dict[str, Any] = {
        "endpoint_url": endpoint,
    }
    key = storage_options.get("access_key_id") or storage_options.get("AWS_ACCESS_KEY_ID", "")
    secret = storage_options.get("secret_access_key") or storage_options.get(
        "AWS_SECRET_ACCESS_KEY", ""
    )
    if key:
        options["key"] = key
    if secret:
        options["secret"] = secret
    return options
