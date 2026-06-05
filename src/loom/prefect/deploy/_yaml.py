"""Load per-ETL YAML configuration with chained ``extends`` support."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def read_yaml(config_path: str) -> dict[str, Any]:
    """Read a per-ETL YAML resolving any chain of ``extends:`` references.

    Args:
        config_path: Path to the per-ETL YAML file.

    Returns:
        Merged top-level mapping, with the ``extends`` key stripped.

    Raises:
        ValueError: When the top level is not a mapping or the
            ``extends`` chain forms a cycle.
    """
    from omegaconf import DictConfig, OmegaConf  # noqa: PLC0415

    merged = _merge_chain(Path(config_path).resolve(), visited=[])
    container = (
        OmegaConf.to_container(merged, resolve=False) if isinstance(merged, DictConfig) else merged
    )
    if not isinstance(container, dict):
        raise ValueError(f"{config_path}: top-level YAML must be a mapping")
    container.pop("extends", None)
    return {str(k): v for k, v in container.items()}


def _merge_chain(path: Path, visited: list[Path]) -> Any:
    from omegaconf import DictConfig, OmegaConf  # noqa: PLC0415

    if path in visited:
        chain = " -> ".join(str(p) for p in [*visited, path])
        raise ValueError(f"extends cycle detected: {chain}")
    raw = OmegaConf.load(str(path))
    extends_target = raw.get("extends") if isinstance(raw, DictConfig) else None
    if not extends_target:
        return raw
    base_path = (path.parent / str(extends_target)).resolve()
    base = _merge_chain(base_path, [*visited, path])
    return OmegaConf.merge(base, raw)


__all__ = ["read_yaml"]
