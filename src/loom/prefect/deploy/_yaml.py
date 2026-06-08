"""Per-ETL YAML loader — thin adapter over ``loom.core.config.load_config``."""

from __future__ import annotations

import re
from typing import Any

from loom.core.config import load_config

_ENV_INTERPOLATION = re.compile(r"\$\{oc\.[^}]*\}")


def read_yaml(config_path: str) -> dict[str, Any]:
    """Load a per-ETL YAML via ``loom.core.config.load_config``.

    ``${oc.*}`` interpolations are resolved against the host env at load
    time; loom placeholders (``${now}``, ``${today-1d}``, …) are kept
    verbatim so :func:`resolve_placeholder` resolves them at flow-run
    time. ``s3://``/``gs://``/… URIs are honoured.

    Raises:
        ValueError: When the top level is not a mapping.
        loom.core.config.errors.ConfigError: Underlying load failures.
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
    except OSError:
        return
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
    from omegaconf.errors import OmegaConfBaseException  # noqa: PLC0415

    try:
        node = OmegaConf.create({"_": value})
        resolved = OmegaConf.to_container(node, resolve=True)
    except OmegaConfBaseException:
        return value
    if isinstance(resolved, dict):
        return resolved.get("_", value)
    return value


__all__ = ["read_yaml"]
