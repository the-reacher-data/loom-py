"""Config loading helpers for ETLRunner factory constructors."""

from __future__ import annotations

from typing import Any

import msgspec
from omegaconf import DictConfig, OmegaConf

from loom.etl.observability.config import ObservabilityConfig
from loom.etl.storage._config import StorageConfig, convert_storage_config


def _read_yaml_file(path: str) -> str:
    """Read raw YAML text from a local filesystem path."""
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def _parse_yaml_content(content: str) -> tuple[StorageConfig, ObservabilityConfig]:
    """Parse and resolve a YAML string into typed config objects.

    Args:
        content: Raw YAML text (``storage:`` key required).

    Returns:
        Tuple of validated :data:`~loom.etl.StorageConfig` and
        :class:`~loom.etl.ObservabilityConfig`.

    Raises:
        KeyError: When ``storage:`` key is absent.
        msgspec.ValidationError: When config shape is invalid.
    """
    created = OmegaConf.create(content)
    if not isinstance(created, DictConfig):
        raise TypeError(
            f"Expected a mapping from OmegaConf.create, got {type(created).__name__}. "
            "Pass a dict or YAML string with a top-level mapping."
        )
    raw: DictConfig = created
    storage_raw: Any = OmegaConf.to_container(raw["storage"], resolve=True)
    storage_config = convert_storage_config(storage_raw)

    obs_config = ObservabilityConfig()
    if "observability" in raw:
        obs_raw: Any = OmegaConf.to_container(raw["observability"], resolve=True)
        obs_config = msgspec.convert(obs_raw, ObservabilityConfig)

    return storage_config, obs_config


def _load_yaml(path: str) -> tuple[StorageConfig, ObservabilityConfig]:
    """Read a YAML file from the filesystem and parse it."""
    return _parse_yaml_content(_read_yaml_file(path))
