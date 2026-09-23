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


_ENCRYPTION_KEYS = (
    "aws_server_side_encryption",
    "aws_sse_kms_key_id",
    "aws_sse_bucket_key_enabled",
)

_S3_ENCRYPTION_PARAMS = {
    "aws_server_side_encryption": "ServerSideEncryption",
    "aws_sse_kms_key_id": "SSEKMSKeyId",
    "aws_sse_bucket_key_enabled": "BucketKeyEnabled",
}

_TRUTHY = frozenset({"1", "true", "yes", "on"})


def _encryption_options(storage_options: Mapping[str, str]) -> dict[str, str]:
    """Return the declared server-side encryption options, lowercase-keyed.

    Both the lowercase (delta-rs/object_store) and uppercase (environment
    variable) spellings are accepted, as for endpoint and credentials.
    """
    found: dict[str, str] = {}
    for name in _ENCRYPTION_KEYS:
        value = storage_options.get(name) or storage_options.get(name.upper(), "")
        if value:
            found[name] = value
    return found


def _s3_additional_kwargs(encryption: Mapping[str, str]) -> dict[str, Any]:
    """Translate encryption options into the botocore parameters s3fs forwards.

    fsspec/s3fs does not understand the object_store key names: it forwards
    ``s3_additional_kwargs`` verbatim to botocore, where ``BucketKeyEnabled``
    is a boolean shape and would be refused as a string.
    """
    params: dict[str, Any] = {}
    for name, param in _S3_ENCRYPTION_PARAMS.items():
        value = encryption.get(name)
        if not value:
            continue
        params[param] = value.strip().lower() in _TRUTHY if param == "BucketKeyEnabled" else value
    return params


def _checkpoint_storage_options(storage_options: Mapping[str, str]) -> dict[str, Any]:
    """Return fsspec-compatible options for checkpoint cleanup/write paths.

    Only the options fsspec understands survive: an S3-compatible endpoint with
    its credentials, and the server-side encryption parameters, which fsspec
    takes as botocore kwargs rather than under their object_store names.  Any
    other entry of the delta-rs style mapping is dropped, and logged at debug.
    """
    options: dict[str, Any] = {}
    consumed = {"endpoint_url", "AWS_ENDPOINT_URL"}

    endpoint = storage_options.get("endpoint_url") or storage_options.get("AWS_ENDPOINT_URL", "")
    if endpoint:
        options["endpoint_url"] = endpoint
        key = storage_options.get("access_key_id") or storage_options.get("AWS_ACCESS_KEY_ID", "")
        secret = storage_options.get("secret_access_key") or storage_options.get(
            "AWS_SECRET_ACCESS_KEY", ""
        )
        if key:
            options["key"] = key
        if secret:
            options["secret"] = secret
    consumed |= {
        "access_key_id",
        "AWS_ACCESS_KEY_ID",
        "secret_access_key",
        "AWS_SECRET_ACCESS_KEY",
    }

    encryption = _encryption_options(storage_options)
    if encryption:
        options["s3_additional_kwargs"] = _s3_additional_kwargs(encryption)
    consumed |= set(_ENCRYPTION_KEYS) | {name.upper() for name in _ENCRYPTION_KEYS}

    dropped = sorted(set(storage_options) - consumed)
    if dropped:
        _log.debug("checkpoint storage options ignored keys=%s", dropped)
    return options
