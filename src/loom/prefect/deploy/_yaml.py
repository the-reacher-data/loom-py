"""Per-ETL YAML loader — thin adapter over ``loom.core.config.load_config``."""

from __future__ import annotations

import re
from typing import Any

from loom.core.config import load_config

_ENV_INTERPOLATION = re.compile(r"\$\{oc\.[^}]*\}")


def read_yaml(config_path: str) -> dict[str, Any]:
    """Load a per-ETL YAML using loom's canonical config loader.

    Supports the framework-wide ``includes:`` directive, ``${oc.env:VAR}``
    interpolation, custom resolvers, and cloud URIs (``s3://``, ``gs://``…)
    — same surface as the storage YAML loaded by ``ETLRunner.from_yaml``.

    Loom-runtime placeholders (``${now}``, ``${today-1d}``, …) are
    deliberately left untouched here so :func:`resolve_placeholder`
    materialises them at flow-run time against the bound parameters.

    Args:
        config_path: Local path or cloud URI to the per-ETL YAML.

    Returns:
        Merged top-level mapping with ``${oc.*}`` interpolations resolved,
        loom placeholders preserved verbatim, and the ``includes`` key
        stripped.

    Raises:
        ValueError: When the top level is not a mapping.
        loom.core.config.errors.ConfigError: Underlying load failures
            (missing file, circular include, parse error).
    """
    from omegaconf import DictConfig, OmegaConf  # noqa: PLC0415

    _ensure_mapping_top_level(config_path)
    merged = load_config(config_path)
    if not isinstance(merged, DictConfig):
        raise ValueError(f"{config_path}: top-level YAML must be a mapping")
    container = OmegaConf.to_container(merged, resolve=False)
    if not isinstance(container, dict):
        raise ValueError(f"{config_path}: top-level YAML must be a mapping")
    container.pop("includes", None)
    return {str(k): _resolve_oc_only(v) for k, v in container.items()}


def _ensure_mapping_top_level(config_path: str) -> None:
    from omegaconf import DictConfig, OmegaConf  # noqa: PLC0415

    try:
        raw = OmegaConf.load(config_path)
    except Exception:  # noqa: BLE001
        return  # let load_config produce the canonical error
    if not isinstance(raw, DictConfig):
        raise ValueError(f"{config_path}: top-level YAML must be a mapping")


def _resolve_oc_only(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _resolve_oc_only(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_oc_only(v) for v in value]
    if isinstance(value, str) and _ENV_INTERPOLATION.search(value):
        return _resolve_string(value)
    return value


def _resolve_string(value: str) -> Any:
    from omegaconf import OmegaConf  # noqa: PLC0415

    try:
        node = OmegaConf.create({"_": value})
        resolved = OmegaConf.to_container(node, resolve=True)
        if isinstance(resolved, dict):
            return resolved.get("_", value)
        return value
    except Exception:  # noqa: BLE001
        return value


__all__ = ["read_yaml"]
