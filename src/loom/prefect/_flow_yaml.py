"""Per-flow YAML loading shared by the flow factories and the deployer.

Lives outside both ``loom.prefect.flow`` and ``loom.prefect.deploy`` so that
the flow package imports nothing from the deploy package.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from loom.core.config import is_cloud_uri, load_config

_ENV_INTERPOLATION = re.compile(r"\$\{oc\.[^}]*\}")


def read_yaml(config_path: str) -> dict[str, Any]:
    """Load a per-flow YAML via ``loom.core.config.load_config``.

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
    merged = load_config(config_path, keyed=("etls",))
    if not isinstance(merged, DictConfig):
        raise ValueError(f"{config_path}: top-level YAML must be a mapping")
    container = OmegaConf.to_container(merged, resolve=False)
    if not isinstance(container, dict):
        raise ValueError(f"{config_path}: top-level YAML must be a mapping")
    container.pop("includes", None)
    return {str(k): _resolve_oc_only(v) for k, v in container.items()}


def resolve_config_uri(config_path: str) -> str:
    """Return a cloud URI verbatim and a local path resolved to an absolute one.

    Args:
        config_path: Local path (optionally a glob) or cloud URI.

    Returns:
        The value recorded in flow metadata and job variables.
    """
    if is_cloud_uri(config_path):
        return config_path
    return str(Path(config_path).resolve())


def extract_pool_config(raw_cfg: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Return per-environment work-pool overrides from the per-flow YAML.

    Args:
        raw_cfg: Top-level YAML mapping returned by :func:`read_yaml`.

    Returns:
        Mapping ``environment → {"work_pool": str | None,
        "job_variables": dict}``. Empty when the YAML has no
        ``environments`` block.
    """
    environments = raw_cfg.get("environments") or {}
    out: dict[str, dict[str, Any]] = {}
    for env_name, env_block in environments.items():
        if not isinstance(env_block, dict):
            continue
        out[env_name] = {
            "work_pool": env_block.get("work_pool"),
            "job_variables": env_block.get("job_variables", {}) or {},
        }
    return out


def _ensure_mapping_top_level(config_path: str) -> None:
    """Reject a non-mapping document; unreadable or malformed files are left to ``load_config``."""
    import yaml  # noqa: PLC0415
    from omegaconf import DictConfig, OmegaConf  # noqa: PLC0415

    try:
        raw = OmegaConf.load(config_path)
    except (OSError, yaml.YAMLError):
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


__all__ = ["extract_pool_config", "read_yaml", "resolve_config_uri"]
