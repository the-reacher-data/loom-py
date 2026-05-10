"""Config loading helpers for ETLRunner factory constructors."""

from __future__ import annotations

from loom.core.config import ConfigContext, ConfigKey
from loom.etl.lineage._config import ETLObservabilityConfig
from loom.etl.storage._config import StorageConfig


def _load_yaml(path: str) -> tuple[StorageConfig, ETLObservabilityConfig]:
    """Load and parse an ETL config YAML.

    Accepts local filesystem paths and cloud storage URIs
    (``s3://``, ``gs://``, ``abfss://``, ``r2://`` …).

    Args:
        path: Local path or cloud URI pointing to a YAML file with a
            top-level ``storage:`` key and an optional ``observability:`` key.

    Returns:
        Tuple of validated :data:`~loom.etl.StorageConfig` and
        :class:`~loom.etl.lineage.ETLObservabilityConfig`.

    Raises:
        KeyError: When the ``storage:`` key is absent.
        msgspec.ValidationError: When the config shape is invalid.
        loom.core.config.ConfigError: When the file cannot be read or parsed.
    """
    ctx = ConfigContext.from_yaml(path)
    storage_config = ctx.section(ConfigKey.STORAGE, StorageConfig)
    obs_config = ctx.section_or_default(
        ConfigKey.OBSERVABILITY,
        ETLObservabilityConfig,
        ETLObservabilityConfig(),
    )

    return storage_config, obs_config
