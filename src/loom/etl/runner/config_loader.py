"""Config loading helpers for ETLRunner factory constructors."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import msgspec

from loom.core.config import (
    ConfigContext,
    ConfigError,
    ConfigKey,
    ConfigResolver,
    default_resolvers,
    merge_resolvers,
)
from loom.etl.lineage._config import ETLObservabilityConfig
from loom.etl.storage._config import (
    STORAGE_KEYED_COLLECTIONS,
    StorageConfig,
    normalise_storage_section,
)


def _load_yaml(
    path: str, *, resolvers: Sequence[ConfigResolver] = ()
) -> tuple[StorageConfig, ETLObservabilityConfig]:
    """Load and parse an ETL config YAML.

    Accepts local filesystem paths and cloud storage URIs
    (``s3://``, ``gs://``, ``abfss://``, ``r2://`` …).  ``storage.tables``
    and ``storage.files`` accept the list form or a mapping keyed by logical
    name; ``storage.tables``, ``storage.files`` and ``storage.profiles`` are
    merged by key across ``includes``.
    A ``path.profile: <name>`` key is replaced by the fields of
    ``storage.profiles[<name>]`` the path does not set itself.

    Args:
        path: Local path or cloud URI pointing to a YAML file with a
            top-level ``storage:`` key and an optional ``observability:`` key.
        resolvers: Resolvers for ``${name:key}`` placeholders, registered
            before the built-in ``secrets`` and ``ssm`` defaults.  A resolver
            named like a default replaces it.

    Returns:
        Tuple of validated :data:`~loom.etl.StorageConfig` and
        :class:`~loom.etl.lineage.ETLObservabilityConfig`.

    Raises:
        loom.core.config.ConfigError: When the file cannot be read or parsed,
            the ``storage:`` key is absent, a keyed collection holds a
            duplicate key or mixes list and mapping forms, or the config
            shape is invalid.
        ValueError: When a mapping entry carries a ``name`` different from
            its key, a ``path.profile`` is not declared in ``storage.profiles``,
            or a profile carries ``uri`` or an unknown field.
    """
    ctx = ConfigContext.from_yaml(
        path,
        resolvers=merge_resolvers(resolvers, default_resolvers()),
        keyed=STORAGE_KEYED_COLLECTIONS,
    )
    raw = ctx.section(ConfigKey.STORAGE, dict)
    storage_config = _convert_storage(normalise_storage_section(raw))
    obs_config = ctx.section_or_default(
        ConfigKey.OBSERVABILITY,
        ETLObservabilityConfig,
        ETLObservabilityConfig(),
    )

    return storage_config, obs_config


def _convert_storage(raw: dict[str, Any]) -> StorageConfig:
    """Convert the normalised ``storage:`` section leniently.

    Args:
        raw: Normalised ``storage:`` section.

    Returns:
        The bound :class:`StorageConfig`.

    Raises:
        loom.core.config.ConfigError: When the section fails validation.
    """
    try:
        return msgspec.convert(raw, StorageConfig, strict=False)
    except msgspec.ValidationError as exc:
        raise ConfigError(
            f"Config section {ConfigKey.STORAGE.value!r} failed validation as "
            f"{StorageConfig.__name__!r}: {exc}"
        ) from exc
