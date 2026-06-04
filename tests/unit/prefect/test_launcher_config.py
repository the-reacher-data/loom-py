"""Tests for loom.prefect._launcher_config.

Covers:
- ``_load_etl_yaml`` extends/deep-merge semantics.
- Lazy per-environment resolution of ${oc.env:VAR}.
- ``build_launcher`` dispatch to FargateLauncher / LocalDockerLauncher.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from loom.prefect._docker_launcher import LocalDockerLauncher
from loom.prefect._fargate_launcher import FargateLauncher
from loom.prefect._launcher_config import _load_etl_yaml, build_launcher


def _write(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def test_load_yaml_without_extends(tmp_path: Path) -> None:
    cfg_file = tmp_path / "etl.yaml"
    _write(
        cfg_file,
        """
etl: solo
environments:
  local:
    type: docker
    network: loom_net
""",
    )
    cfg = _load_etl_yaml(str(cfg_file))
    assert cfg["etl"] == "solo"
    assert cfg["environments"]["local"]["type"] == "docker"


def test_load_yaml_deep_merges_extends(tmp_path: Path) -> None:
    base = tmp_path / "base.yaml"
    _write(
        base,
        """
environments:
  prod:
    type: fargate
    cluster: base-cluster
    assign_public_ip: false
    subnets: ["subnet-base"]
  local:
    type: docker
    network: base_net
""",
    )
    child = tmp_path / "etl.yaml"
    _write(
        child,
        """
extends: ./base.yaml
etl: child
environments:
  prod:
    cluster: child-cluster
    subnets: ["subnet-child-a", "subnet-child-b"]
""",
    )
    cfg = _load_etl_yaml(str(child))
    prod = cfg["environments"]["prod"]
    # Scalar override.
    assert prod["cluster"] == "child-cluster"
    # Dict deep-merge: base-only scalar preserved.
    assert prod["assign_public_ip"] is False
    assert prod["type"] == "fargate"
    # Lists replace (no concat).
    subnets = list(prod["subnets"])
    assert subnets == ["subnet-child-a", "subnet-child-b"]
    # Untouched envs survive from base.
    assert cfg["environments"]["local"]["network"] == "base_net"


def test_load_yaml_extends_missing_file_raises(tmp_path: Path) -> None:
    child = tmp_path / "etl.yaml"
    _write(child, "extends: ./does_not_exist.yaml\netl: x\n")
    with pytest.raises(FileNotFoundError):
        _load_etl_yaml(str(child))


def test_build_launcher_returns_fargate_for_fargate_type(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg_file = tmp_path / "etl.yaml"
    _write(
        cfg_file,
        """
etl: daily-orders
environments:
  prod:
    type: fargate
    cluster: ${oc.env:LOOM_ECS_CLUSTER}
    task_definition: my-td:1
    subnets: ["subnet-a", "subnet-b"]
    security_groups: ["sg-1"]
    assign_public_ip: false
""",
    )
    monkeypatch.setenv("LOOM_ECS_CLUSTER", "resolved-cluster")
    launcher = build_launcher(config_path=str(cfg_file), environment="prod")
    assert isinstance(launcher, FargateLauncher)


def test_build_launcher_returns_docker_for_docker_type(tmp_path: Path) -> None:
    cfg_file = tmp_path / "etl.yaml"
    _write(
        cfg_file,
        """
etl: daily-orders
environments:
  local:
    type: docker
    image: my-etl:dev
    network: loom_net
""",
    )
    launcher = build_launcher(config_path=str(cfg_file), environment="local")
    assert isinstance(launcher, LocalDockerLauncher)


def test_build_launcher_unknown_type_raises_value_error(tmp_path: Path) -> None:
    cfg_file = tmp_path / "etl.yaml"
    _write(
        cfg_file,
        """
etl: daily-orders
environments:
  weird:
    type: kubernetes
""",
    )
    with pytest.raises(ValueError):
        build_launcher(config_path=str(cfg_file), environment="weird")


def test_build_launcher_missing_env_var_in_selected_env_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg_file = tmp_path / "etl.yaml"
    _write(
        cfg_file,
        """
etl: x
environments:
  prod:
    type: fargate
    cluster: ${oc.env:LOOM_ECS_CLUSTER_MISSING}
    task_definition: td:1
    subnets: ["subnet-a"]
    security_groups: ["sg-1"]
""",
    )
    monkeypatch.delenv("LOOM_ECS_CLUSTER_MISSING", raising=False)
    # OmegaConf raises InterpolationResolutionError (subclass of ValueError)
    # when ${oc.env:VAR} cannot be resolved for the selected environment.
    with pytest.raises(ValueError):
        build_launcher(config_path=str(cfg_file), environment="prod")


def test_build_launcher_does_not_resolve_unselected_environments(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg_file = tmp_path / "etl.yaml"
    _write(
        cfg_file,
        """
etl: x
environments:
  prod:
    type: fargate
    cluster: ${oc.env:LOOM_ECS_CLUSTER_UNSET}
    task_definition: td:1
    subnets: ["subnet-a"]
    security_groups: ["sg-1"]
  local:
    type: docker
    image: my-etl:dev
    network: loom_net
""",
    )
    monkeypatch.delenv("LOOM_ECS_CLUSTER_UNSET", raising=False)
    # Loading the local env must NOT trigger resolution of prod's env var.
    launcher = build_launcher(config_path=str(cfg_file), environment="local")
    assert isinstance(launcher, LocalDockerLauncher)
