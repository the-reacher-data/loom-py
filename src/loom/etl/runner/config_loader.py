"""Config loading helpers for ETLRunner factory constructors."""

from __future__ import annotations

from typing import Any

import msgspec

from loom.core.config.loader import load_config
from loom.etl.lineage._config import ETLObservabilityConfig
from loom.etl.storage._config import StorageConfig, convert_storage_config


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
    from omegaconf import OmegaConf

    cfg = load_config(path)

    storage_raw: Any = OmegaConf.to_container(cfg["storage"], resolve=True)
    storage_config = convert_storage_config(storage_raw)

    obs_config = ETLObservabilityConfig()
    if "observability" in cfg:
        obs_raw: Any = OmegaConf.to_container(cfg["observability"], resolve=True)
        obs_config = msgspec.convert(obs_raw, ETLObservabilityConfig)

    return storage_config, obs_config
