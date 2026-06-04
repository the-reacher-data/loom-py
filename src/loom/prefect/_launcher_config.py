"""YAML-driven configuration loader and launcher factory.

Loads a per-ETL YAML, applies a single level of ``extends`` deep-merge,
and lazily resolves only the selected ``environments.<env>`` subnode (so
unrelated environments can reference env vars that are not present).

Trust boundary
--------------
The YAML files this module loads are part of the project repository and
reviewed via the same code-review process as the source files (CI/CD).
This module does NOT defend against adversarial YAML — in particular:

- ``extends:`` is resolved relative to the YAML's parent directory and not
  contained to any project root. A malicious YAML could read any file the
  Prefect worker has access to.
- ``${oc.env:VAR}`` interpolation can pull any process environment variable
  into the rendered config (and hence into ECS tags / container env vars).
  Avoid placing secrets in worker environment if YAML authorship is not
  trusted.

If you ever need to load YAML from a less-trusted source (e.g. user
upload), wrap the loader with explicit allow-lists.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from omegaconf import DictConfig, OmegaConf

from loom.prefect._docker_launcher import DockerConfig, LocalDockerLauncher
from loom.prefect._fargate_launcher import FargateConfig, FargateLauncher
from loom.prefect._launcher import ContainerLauncher


def _deep_merge(base: Any, override: Any) -> Any:
    """Deep-merge two raw containers; lists in ``override`` REPLACE base lists."""
    if isinstance(base, dict) and isinstance(override, dict):
        merged: dict[str, Any] = dict(base)
        for key, value in override.items():
            if key in merged:
                merged[key] = _deep_merge(merged[key], value)
            else:
                merged[key] = value
        return merged
    return override


def _load_etl_yaml(path: str) -> DictConfig:
    """Load a YAML config, resolving a single level of ``extends:``.

    Args:
        path: Filesystem path to the YAML file.

    Returns:
        An unresolved :class:`omegaconf.DictConfig`. ``${oc.env:...}``
        references are deliberately NOT resolved here so callers can pick
        a single environment without forcing every env var to be defined.

    Raises:
        FileNotFoundError: When ``path`` or the file referenced by
            ``extends`` does not exist.
    """
    cfg_path = Path(path)
    if not cfg_path.exists():
        raise FileNotFoundError(path)
    child_cfg = OmegaConf.load(str(cfg_path))
    raw_child: Any = OmegaConf.to_container(child_cfg, resolve=False)
    if not isinstance(raw_child, dict):
        raise ValueError(f"{path}: top-level YAML must be a mapping")

    extends = raw_child.pop("extends", None)
    if extends is None:
        merged_dict: dict[str, Any] = raw_child
    else:
        base_path = (cfg_path.parent / extends).resolve()
        if not base_path.exists():
            raise FileNotFoundError(str(base_path))
        base_cfg = OmegaConf.load(str(base_path))
        raw_base: Any = OmegaConf.to_container(base_cfg, resolve=False)
        if not isinstance(raw_base, dict):
            raise ValueError(f"{base_path}: top-level YAML must be a mapping")
        merged_dict = _deep_merge(raw_base, raw_child)

    merged = OmegaConf.create(merged_dict)
    assert isinstance(merged, DictConfig)
    return merged


def _resolved_environment(cfg: DictConfig, environment: str) -> dict[str, Any]:
    """Resolve only ``environments.<environment>`` to plain python types."""
    envs = cfg.get("environments")
    if envs is None or environment not in envs:
        raise KeyError(f"environment {environment!r} not in config")
    env_node = envs[environment]
    resolved: Any = OmegaConf.to_container(env_node, resolve=True)
    if not isinstance(resolved, dict):
        raise ValueError(f"environments.{environment} must resolve to a mapping")
    return resolved


def build_launcher(
    *,
    config_path: str,
    environment: str,
) -> ContainerLauncher:
    """Load the YAML, select an environment, and instantiate the right backend.

    Args:
        config_path: Path to the per-ETL YAML.
        environment: Environment key under ``environments``.

    Returns:
        A concrete :class:`ContainerLauncher` (Fargate or Docker).

    Raises:
        KeyError: ``environment`` not declared in the YAML.
        ValueError: ``type`` field missing or unknown.
        omegaconf.errors.InterpolationKeyError: Required env var ABSENT
            for the selected environment.
    """
    cfg = _load_etl_yaml(config_path)
    env_cfg = _resolved_environment(cfg, environment)
    backend = env_cfg.pop("type", None)
    if backend is None:
        raise ValueError(f"environments.{environment}.type is required")

    etl_name_raw = cfg.get("etl", "")
    etl_name = str(etl_name_raw) if etl_name_raw is not None else ""

    if backend == "fargate":
        fargate_cfg = FargateConfig(
            cluster=env_cfg["cluster"],
            task_definition=env_cfg["task_definition"],
            subnets=tuple(env_cfg.get("subnets", ())),
            security_groups=tuple(env_cfg.get("security_groups", ())),
            container_name=env_cfg.get("container_name"),
            assign_public_ip=bool(env_cfg.get("assign_public_ip", False)),
            capacity_provider=env_cfg.get("capacity_provider"),
            etl_name=etl_name,
        )
        return FargateLauncher(fargate_cfg)
    if backend == "docker":
        volumes_raw = env_cfg.get("volumes", ()) or ()
        volumes = tuple((str(h), str(c)) for h, c in volumes_raw)
        docker_cfg = DockerConfig(
            image=env_cfg["image"],
            network=env_cfg.get("network"),
            cpu=env_cfg.get("cpu"),
            memory=env_cfg.get("memory"),
            volumes=volumes,
            etl_name=etl_name,
        )
        return LocalDockerLauncher(docker_cfg)
    raise ValueError(f"unknown launcher type: {backend!r}")


__all__ = ["build_launcher"]
