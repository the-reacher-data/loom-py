"""Tests for loom.prefect._docker_launcher.LocalDockerLauncher.

Mocks docker.DockerClient and the containers.* APIs. No real docker.sock
required.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from loom.prefect._docker_launcher import DockerConfig, LocalDockerLauncher
from loom.prefect._launcher import (
    ContainerExecution,
    ContainerLauncher,
    ContainerResult,
    ContainerTaskTimeoutError,
)


def _make_client(container: MagicMock | None = None) -> MagicMock:
    client = MagicMock()
    container = container or MagicMock()
    container.id = "cont-1"
    client.containers.run.return_value = container
    client.containers.get.return_value = container
    return client


def _basic_config(**overrides) -> DockerConfig:
    defaults: dict[str, Any] = {
        "image": "my-etl:dev",
        "network": "loom_net",
        "cpu": None,
        "memory": None,
        "volumes": (),
        "etl_name": "daily-orders",
    }
    defaults.update(overrides)
    return DockerConfig(**defaults)


def test_local_docker_launcher_satisfies_protocol() -> None:
    launcher = LocalDockerLauncher(_basic_config(), docker_client=_make_client())
    assert isinstance(launcher, ContainerLauncher)


def test_submit_invokes_containers_run_with_expected_args() -> None:
    client = _make_client()
    launcher = LocalDockerLauncher(_basic_config(), docker_client=client)

    execution = launcher.submit(
        env={"LOOM_FLOW_CTX_JSON": "{}", "LOOM_FLOW_PARAMS_JSON": "{}"},
        tags={"loom.correlation_id": "corr-1"},
    )
    assert isinstance(execution, ContainerExecution)
    assert execution.id == "cont-1"
    assert execution.logs_url is None

    client.containers.run.assert_called_once()
    kwargs = client.containers.run.call_args.kwargs
    assert (
        kwargs["image"] == "my-etl:dev" or client.containers.run.call_args.args[0] == "my-etl:dev"
    )
    assert kwargs["environment"] == {
        "LOOM_FLOW_CTX_JSON": "{}",
        "LOOM_FLOW_PARAMS_JSON": "{}",
    }
    assert kwargs["detach"] is True
    assert kwargs["network"] == "loom_net"
    labels = kwargs["labels"]
    assert labels["loom.etl"] == "daily-orders"
    assert labels["loom.correlation_id"] == "corr-1"


def test_submit_translates_cpu_and_memory_limits() -> None:
    client = _make_client()
    cfg = _basic_config(cpu="2", memory="1g")
    launcher = LocalDockerLauncher(cfg, docker_client=client)

    launcher.submit(
        env={"LOOM_FLOW_CTX_JSON": "{}", "LOOM_FLOW_PARAMS_JSON": "{}"},
    )
    kwargs = client.containers.run.call_args.kwargs
    # nano_cpus = float(cpu) * 1e9 (spec).
    assert kwargs.get("nano_cpus") == int(2 * 1e9)
    assert kwargs.get("mem_limit") == "1g"


def test_wait_returns_result_from_container_wait() -> None:
    container = MagicMock()
    container.id = "cont-1"
    container.wait.return_value = {"StatusCode": 0}
    container.attrs = {"State": {"Error": ""}}
    client = _make_client(container)

    launcher = LocalDockerLauncher(_basic_config(), docker_client=client)
    result = launcher.wait(
        ContainerExecution(id="cont-1", logs_url=None),
        poll_interval_seconds=0.01,
        timeout_seconds=60,
    )
    assert isinstance(result, ContainerResult)
    assert result.exit_code == 0
    assert result.reason is None


def test_wait_surfaces_state_error_as_reason() -> None:
    container = MagicMock()
    container.id = "cont-1"
    container.wait.return_value = {"StatusCode": 137}
    container.attrs = {"State": {"Error": "OOMKilled"}}
    client = _make_client(container)

    launcher = LocalDockerLauncher(_basic_config(), docker_client=client)
    result = launcher.wait(
        ContainerExecution(id="cont-1", logs_url=None),
        poll_interval_seconds=0.01,
        timeout_seconds=60,
    )
    assert result.exit_code == 137
    assert result.reason == "OOMKilled"


def test_wait_raises_timeout_on_read_timeout() -> None:
    import requests

    container = MagicMock()
    container.id = "cont-1"
    container.wait.side_effect = requests.exceptions.ReadTimeout("timed out")
    client = _make_client(container)

    launcher = LocalDockerLauncher(_basic_config(), docker_client=client)
    with pytest.raises(ContainerTaskTimeoutError):
        launcher.wait(
            ContainerExecution(id="cont-1", logs_url=None),
            poll_interval_seconds=0.01,
            timeout_seconds=1,
        )


def test_cancel_invokes_container_kill() -> None:
    container = MagicMock()
    container.id = "cont-1"
    client = _make_client(container)

    launcher = LocalDockerLauncher(_basic_config(), docker_client=client)
    launcher.cancel(ContainerExecution(id="cont-1", logs_url=None))
    container.kill.assert_called_once()


def test_cancel_idempotent_on_not_found() -> None:
    import docker.errors

    container = MagicMock()
    container.id = "cont-1"
    container.kill.side_effect = docker.errors.NotFound("gone")
    client = _make_client(container)

    launcher = LocalDockerLauncher(_basic_config(), docker_client=client)
    # Must not raise.
    launcher.cancel(ContainerExecution(id="cont-1", logs_url=None))


def test_cancel_idempotent_on_api_error() -> None:
    import docker.errors

    container = MagicMock()
    container.id = "cont-1"
    container.kill.side_effect = docker.errors.APIError("nope")
    client = _make_client(container)

    launcher = LocalDockerLauncher(_basic_config(), docker_client=client)
    launcher.cancel(ContainerExecution(id="cont-1", logs_url=None))
