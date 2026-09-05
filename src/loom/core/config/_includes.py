"""Resolution and glob expansion of ``includes`` entries on any scheme.

Local entries use the standard library only; cloud entries import
``fsspec`` lazily, so local configurations load without the extra.
"""

from __future__ import annotations

import glob
import os
import posixpath
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from loom.core.config.errors import ConfigError

_CLOUD_SCHEMES = frozenset({"s3", "gs", "gcs", "abfss", "abfs", "az", "r2", "memory"})
_CONFIG_SUFFIXES = frozenset({".yaml", ".yml"})


def is_cloud_uri(uri: str) -> bool:
    """Return ``True`` when *uri* uses a cloud storage scheme.

    Args:
        uri: Local path or URI.

    Returns:
        Whether the scheme belongs to a cloud storage backend.
    """
    return urlsplit(str(uri).strip()).scheme.lower() in _CLOUD_SCHEMES


def resolve_include(base: str, entry: str) -> str:
    """Resolve an ``includes`` entry against the file that declares it.

    Args:
        base: Path or URI of the declaring file.
        entry: Include entry as written: a URI, an absolute path, or a path
            relative to the declaring file.

    Returns:
        The entry as is when it carries a scheme or is absolute; otherwise
        the entry joined to the parent of ``base`` and normalised.
    """
    if is_cloud_uri(entry) or os.path.isabs(entry):
        return entry
    if not is_cloud_uri(base):
        return os.path.normpath(os.path.join(os.path.dirname(base), entry))
    parts = urlsplit(base)
    joined = posixpath.normpath(posixpath.join(posixpath.dirname(parts.path), entry))
    return urlunsplit((parts.scheme, parts.netloc, joined, "", ""))


def expand_config_glob(pattern: str) -> list[str]:
    """Expand a configuration file pattern on the local or a cloud filesystem.

    Args:
        pattern: Resolved local path or cloud URI, optionally containing glob
            characters (``*``, ``?``, ``[``).

    Returns:
        ``[pattern]`` for a plain entry; for a glob, the matching regular
        ``.yaml``/``.yml`` files sorted lexicographically by resolved path.

    Raises:
        ConfigError: When a glob matches no configuration file or the cloud
            backend fails to list it.
    """
    if not glob.has_magic(pattern):
        return [pattern]
    matches = _expand_cloud_glob(pattern) if is_cloud_uri(pattern) else _expand_local_glob(pattern)
    if not matches:
        raise ConfigError(f"Include {pattern!r} matches no configuration file")
    return sorted(matches)


def canonical_key(uri: str) -> str:
    """Return the identity of a file used for circular-include detection.

    Args:
        uri: Local path or cloud URI.

    Returns:
        ``scheme://netloc/normalised-path`` for cloud URIs, with an empty
        netloc taken from the first path segment so that ``scheme:///a/b``
        and ``scheme://a/b`` agree; the resolved absolute path for local
        files.
    """
    if not is_cloud_uri(uri):
        return str(Path(uri).resolve())
    parts = urlsplit(uri)
    netloc, path = parts.netloc, parts.path
    if not netloc and path.startswith("/"):
        netloc, _, rest = path.lstrip("/").partition("/")
        path = f"/{rest}"
    return urlunsplit((parts.scheme, netloc, posixpath.normpath(path), "", ""))


def _is_config_file(path: str) -> bool:
    """Return ``True`` when *path* has a YAML suffix."""
    return os.path.splitext(path)[1].lower() in _CONFIG_SUFFIXES


def _expand_local_glob(pattern: str) -> list[str]:
    """Return the regular YAML files matching a local glob pattern."""
    return [p for p in glob.glob(pattern) if os.path.isfile(p) and _is_config_file(p)]


def _expand_cloud_glob(uri: str) -> list[str]:
    """Return the regular YAML files matching a cloud glob, as full URIs."""
    from fsspec.core import url_to_fs

    try:
        fs, path = url_to_fs(uri)
        entries = fs.glob(path, detail=True)
    except Exception as exc:
        raise ConfigError(f"Failed to expand include {uri!r}: {exc}") from exc
    return [
        str(fs.unstrip_protocol(name))
        for name, info in entries.items()
        if info.get("type") == "file" and _is_config_file(name)
    ]
