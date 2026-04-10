"""TempCleaner implementations for cloud-aware intermediate store cleanup.

:class:`TempCleaner` is a structural :class:`typing.Protocol` — any object
with a ``delete_tree(path)`` method satisfies it.  Loom ships three concrete
implementations covering the common environments:

* :class:`LocalTempCleaner`  — local filesystem (``shutil.rmtree``).
* :class:`FsspecTempCleaner` — cloud URIs via ``fsspec`` auto-discovery.
* :class:`AutoTempCleaner`   — default: dispatches by path scheme.

Custom implementations
----------------------
Implement :class:`TempCleaner` for any storage not covered above::

    class MyCustomCleaner:
        def delete_tree(self, path: str) -> None:
            my_storage_client.delete(path, recursive=True)

    store = CheckpointStore(root="custom://...", cleaner=MyCustomCleaner())

Cleanup is always **best-effort** — failures are logged as ``WARNING`` and
never propagate.  Configure a bucket lifecycle / retention policy on
``tmp_root`` as a safety net for environments where cleanup may be unreliable.
"""

from __future__ import annotations

import logging
import os
import shutil
import time
from typing import Protocol, runtime_checkable

_log = logging.getLogger(__name__)

_CLOUD_SCHEMES = ("s3://", "gs://", "gcs://", "abfss://", "abfs://", "az://")


@runtime_checkable
class TempCleaner(Protocol):
    """Structural protocol for deleting a directory tree.

    Any object with a ``delete_tree(path: str) -> None`` method satisfies
    this protocol.  Implement it to support custom storage backends.
    """

    def delete_tree(self, path: str) -> None:
        """Delete *path* and all its contents recursively.

        Implementations must not raise — log a WARNING on failure instead.

        Args:
            path: Absolute local path or cloud URI to delete recursively.
        """
        ...


class LocalTempCleaner:
    """Delete local filesystem trees via ``shutil.rmtree``.

    Also works for Databricks Unity Catalog Volumes (``/Volumes/...``)
    which are mounted as regular filesystem paths.

    Example::

        tmp_root = os.environ["LOOM_TMP_ROOT"]
        cleaner = LocalTempCleaner()
        cleaner.delete_tree(f"{tmp_root}/runs/abc123")
    """

    def delete_tree(self, path: str) -> None:
        """Remove *path* and its contents if it exists.

        Args:
            path: Local directory path to remove.
        """
        if os.path.isdir(path):
            _log.debug("temp cleanup local path=%s", path)
            shutil.rmtree(path, ignore_errors=True)


class FsspecTempCleaner:
    """Delete cloud URI trees via ``fsspec`` credential auto-discovery.

    Relies on credentials configured in the process environment:
    IAM roles, instance profiles, service accounts, or environment
    variables (``AWS_ACCESS_KEY_ID``, ``GOOGLE_APPLICATION_CREDENTIALS``,
    etc.).  Does **not** accept explicit credentials — use :class:`AutoTempCleaner`
    which falls back to this after detecting a cloud URI.

    Requires ``fsspec`` and the appropriate backend (``s3fs``, ``gcsfs``,
    ``adlfs``) to be installed.

    Example::

        cleaner = FsspecTempCleaner()
        cleaner.delete_tree("s3://my-bucket/tmp/loom/runs/abc123")
    """

    def delete_tree(self, path: str) -> None:
        """Remove *path* and its contents from cloud storage.

        Failure is non-fatal — a ``WARNING`` is logged instead.

        Args:
            path: Cloud URI to delete recursively.
        """
        try:
            import fsspec

            fs, fpath = fsspec.core.url_to_fs(path)
            if fs.exists(fpath):
                _log.debug("temp cleanup cloud path=%s", path)
                fs.rm(fpath, recursive=True)
        except Exception as exc:
            _log.warning("temp cloud cleanup skipped path=%s reason=%s", path, exc)


class AutoTempCleaner:
    """Default cleaner: dispatches by path scheme.

    * Local paths (no URI scheme) → :class:`LocalTempCleaner`.
    * Cloud URIs (``s3://``, ``gs://``, ``abfss://``, …) →
      :class:`FsspecTempCleaner`.

    This is the default used by :class:`~loom.etl.checkpoint.CheckpointStore`
    when no explicit cleaner is provided.

    Example::

        tmp_root = os.environ["LOOM_TMP_ROOT"]
        store = CheckpointStore(root=tmp_root)
        # AutoTempCleaner is used automatically — no need to pass it explicitly.
    """

    def __init__(self) -> None:
        self._local = LocalTempCleaner()
        self._cloud = FsspecTempCleaner()

    def delete_tree(self, path: str) -> None:
        """Dispatch to local or cloud cleaner based on *path* scheme.

        Args:
            path: Local path or cloud URI to delete.
        """
        if _is_cloud_path(path):
            self._cloud.delete_tree(path)
        else:
            self._local.delete_tree(path)


def _is_cloud_path(path: str) -> bool:
    """Return ``True`` when *path* starts with a known cloud URI scheme."""
    return path.startswith(_CLOUD_SCHEMES)


def _join_path(base: str, *parts: str) -> str:
    """Join *base* and *parts* into a single path, preserving cloud URI schemes."""
    root = base.rstrip("/")
    suffix = "/".join(part.strip("/") for part in parts if part)
    if not root:
        return f"/{suffix}" if suffix else "/"
    return f"{root}/{suffix}" if suffix else root


def _stale_local_dirs(root: str, *, older_than_seconds: int) -> tuple[str, ...]:
    """Return local child directories older than the given threshold."""
    if not os.path.isdir(root):
        return ()
    cutoff = time.time() - older_than_seconds
    stale: list[str] = []
    for entry in os.scandir(root):
        if entry.is_dir() and entry.stat().st_mtime < cutoff:
            stale.append(entry.path)
    return tuple(stale)
