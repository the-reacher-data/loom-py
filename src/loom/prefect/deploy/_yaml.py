"""Load per-ETL YAML configuration with single-level ``extends``."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def read_yaml(config_path: str) -> dict[str, Any]:
    """Read the per-ETL YAML, applying a single level of ``extends:``.

    Args:
        config_path: Path to the per-ETL YAML file.

    Returns:
        Top-level mapping of the merged YAML, with the ``extends`` key
        stripped.

    Raises:
        ValueError: When the resolved YAML does not have a mapping at the
            top level.
    """
    from omegaconf import DictConfig, OmegaConf  # noqa: PLC0415

    raw = OmegaConf.load(config_path)
    extends_target = None
    if isinstance(raw, DictConfig):
        extends_target = raw.get("extends")
    if extends_target:
        base_path = (Path(config_path).parent / str(extends_target)).resolve()
        base = OmegaConf.load(str(base_path))
        raw = OmegaConf.merge(base, raw)
    container = OmegaConf.to_container(raw, resolve=False)
    if not isinstance(container, dict):
        raise ValueError(f"{config_path}: top-level YAML must be a mapping")
    container.pop("extends", None)
    return {str(k): v for k, v in container.items()}


__all__ = ["read_yaml"]
