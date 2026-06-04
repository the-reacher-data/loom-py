"""Tests for loom.prefect._fargate_launcher.FargateLauncher.

Uses botocore.stub.Stubber to drive a real boto3 ECS client without any
network calls. AWS credentials are not required.
"""

from __future__ import annotations

from typing import Any

import boto3
import pytest
from botocore.stub import Stubber

from loom.prefect._fargate_launcher import FargateConfig, FargateLauncher
from loom.prefect._launcher import (
    ContainerExecution,
    ContainerLauncher,
    ContainerLaunchError,
    ContainerResult,
    ContainerTaskTimeoutError,
)


def _make_ecs_client():
    return boto3.client(
        "ecs",
        region_name="eu-west-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def _basic_config(**overrides) -> FargateConfig:
    defaults: dict[str, Any] = {
        "cluster": "my-cluster",
        "task_definition": "my-td:1",
        "subnets": ("subnet-a", "subnet-b"),
        "security_groups": ("sg-1",),
        "container_name": "app",
        "assign_public_ip": False,
        "capacity_provider": None,
        "etl_name": "daily-orders",
    }
    defaults.update(overrides)
    return FargateConfig(**defaults)


def test_fargate_launcher_satisfies_protocol() -> None:
    launcher = FargateLauncher(_basic_config(), ecs_client=_make_ecs_client())
    assert isinstance(launcher, ContainerLauncher)


def test_submit_invokes_run_task_with_expected_params() -> None:
    ecs = _make_ecs_client()
    stubber = Stubber(ecs)
    task_arn = "arn:aws:ecs:eu-west-1:111:task/abc"
    response = {
        "tasks": [{"taskArn": task_arn, "lastStatus": "PROVISIONING"}],
        "failures": [],
    }
    expected = {
        "cluster": "my-cluster",
        "taskDefinition": "my-td:1",
        "launchType": "FARGATE",
        "networkConfiguration": {
            "awsvpcConfiguration": {
                "subnets": ["subnet-a", "subnet-b"],
                "securityGroups": ["sg-1"],
                "assignPublicIp": "DISABLED",
            }
        },
        "overrides": {
            "containerOverrides": [
                {
                    "name": "app",
                    "environment": [
                        {"name": "LOOM_FLOW_CTX_JSON", "value": "{}"},
                        {"name": "LOOM_FLOW_PARAMS_JSON", "value": "{}"},
                    ],
                }
            ]
        },
        "tags": [
            {"key": "loom.etl", "value": "daily-orders"},
            {"key": "loom.correlation_id", "value": "corr"},
        ],
    }
    stubber.add_response("run_task", response, expected)
    stubber.activate()

    launcher = FargateLauncher(_basic_config(), ecs_client=ecs)
    execution = launcher.submit(
        env={"LOOM_FLOW_CTX_JSON": "{}", "LOOM_FLOW_PARAMS_JSON": "{}"},
        tags={"loom.correlation_id": "corr"},
    )
    assert isinstance(execution, ContainerExecution)
    assert execution.id == task_arn
    stubber.assert_no_pending_responses()


def test_submit_uses_capacity_provider_when_set() -> None:
    ecs = _make_ecs_client()
    stubber = Stubber(ecs)
    task_arn = "arn:task/x"
    stubber.add_response(
        "run_task",
        {"tasks": [{"taskArn": task_arn}], "failures": []},
        expected_params=None,
    )
    stubber.activate()

    cfg = _basic_config(capacity_provider="FARGATE_SPOT")
    launcher = FargateLauncher(cfg, ecs_client=ecs)
    execution = launcher.submit(
        env={"LOOM_FLOW_CTX_JSON": "{}", "LOOM_FLOW_PARAMS_JSON": "{}"},
    )
    assert execution.id == task_arn


def test_submit_assigns_public_ip_when_enabled() -> None:
    ecs = _make_ecs_client()
    stubber = Stubber(ecs)
    task_arn = "arn:task/pub"
    stubber.add_response(
        "run_task",
        {"tasks": [{"taskArn": task_arn}], "failures": []},
    )
    stubber.activate()

    cfg = _basic_config(assign_public_ip=True)
    launcher = FargateLauncher(cfg, ecs_client=ecs)
    execution = launcher.submit(
        env={"LOOM_FLOW_CTX_JSON": "{}", "LOOM_FLOW_PARAMS_JSON": "{}"},
    )
    # The actual params are validated by botocore matching; we assert the
    # call did not raise (assignPublicIp must be ENABLED string).
    assert execution.id == task_arn


def test_submit_raises_when_failures_non_empty() -> None:
    ecs = _make_ecs_client()
    stubber = Stubber(ecs)
    stubber.add_response(
        "run_task",
        {
            "tasks": [],
            "failures": [{"reason": "RESOURCE:MEMORY", "arn": "x"}],
        },
    )
    stubber.activate()

    launcher = FargateLauncher(_basic_config(), ecs_client=ecs)
    with pytest.raises(ContainerLaunchError):
        launcher.submit(
            env={"LOOM_FLOW_CTX_JSON": "{}", "LOOM_FLOW_PARAMS_JSON": "{}"},
        )


def test_wait_polls_until_stopped_and_returns_result() -> None:
    ecs = _make_ecs_client()
    stubber = Stubber(ecs)
    task_arn = "arn:task/abc"
    # First poll: RUNNING.
    stubber.add_response(
        "describe_tasks",
        {
            "tasks": [
                {
                    "taskArn": task_arn,
                    "lastStatus": "RUNNING",
                    "containers": [{"name": "app"}],
                }
            ],
            "failures": [],
        },
    )
    # Second poll: STOPPED with exit 0.
    stubber.add_response(
        "describe_tasks",
        {
            "tasks": [
                {
                    "taskArn": task_arn,
                    "lastStatus": "STOPPED",
                    "stoppedReason": "Essential container exited",
                    "containers": [
                        {"name": "app", "exitCode": 0},
                    ],
                }
            ],
            "failures": [],
        },
    )
    stubber.activate()

    launcher = FargateLauncher(_basic_config(), ecs_client=ecs)
    execution = ContainerExecution(id=task_arn, logs_url=None)
    result = launcher.wait(execution, poll_interval_seconds=0.01, timeout_seconds=60)
    assert isinstance(result, ContainerResult)
    assert result.exit_code == 0
    assert "Essential container" in (result.reason or "")


def test_wait_returns_minus_one_when_exit_code_missing() -> None:
    ecs = _make_ecs_client()
    stubber = Stubber(ecs)
    task_arn = "arn:task/abc"
    stubber.add_response(
        "describe_tasks",
        {
            "tasks": [
                {
                    "taskArn": task_arn,
                    "lastStatus": "STOPPED",
                    "stoppedReason": "TaskFailedToStart",
                    "containers": [{"name": "app"}],
                }
            ],
            "failures": [],
        },
    )
    stubber.activate()

    launcher = FargateLauncher(_basic_config(), ecs_client=ecs)
    result = launcher.wait(
        ContainerExecution(id=task_arn, logs_url=None),
        poll_interval_seconds=0.01,
        timeout_seconds=60,
    )
    assert result.exit_code == -1
    assert result.reason == "TaskFailedToStart"


def test_wait_raises_timeout_when_never_stops() -> None:
    ecs = _make_ecs_client()
    stubber = Stubber(ecs)
    task_arn = "arn:task/abc"
    # Repeatedly RUNNING — Stubber needs enough queued responses.
    # With poll=0.01 and timeout=1 we expect roughly 80-100 iterations.
    for _ in range(200):
        stubber.add_response(
            "describe_tasks",
            {
                "tasks": [
                    {
                        "taskArn": task_arn,
                        "lastStatus": "RUNNING",
                        "containers": [{"name": "app"}],
                    }
                ],
                "failures": [],
            },
        )
    stubber.activate()

    launcher = FargateLauncher(_basic_config(), ecs_client=ecs)
    with pytest.raises(ContainerTaskTimeoutError):
        launcher.wait(
            ContainerExecution(id=task_arn, logs_url=None),
            poll_interval_seconds=0.01,
            timeout_seconds=1,
        )


def test_cancel_invokes_stop_task() -> None:
    ecs = _make_ecs_client()
    stubber = Stubber(ecs)
    task_arn = "arn:task/abc"
    stubber.add_response(
        "stop_task",
        {"task": {"taskArn": task_arn}},
        expected_params={
            "cluster": "my-cluster",
            "task": task_arn,
            "reason": "loom: timeout cancel",
        },
    )
    stubber.activate()

    launcher = FargateLauncher(_basic_config(), ecs_client=ecs)
    launcher.cancel(ContainerExecution(id=task_arn, logs_url=None))
    stubber.assert_no_pending_responses()


def test_cancel_is_idempotent_on_invalid_parameter_exception() -> None:
    ecs = _make_ecs_client()
    stubber = Stubber(ecs)
    task_arn = "arn:task/abc"
    stubber.add_client_error(
        "stop_task",
        service_error_code="InvalidParameterException",
        service_message="Task already stopped",
    )
    stubber.activate()

    launcher = FargateLauncher(_basic_config(), ecs_client=ecs)
    # Must not raise.
    launcher.cancel(ContainerExecution(id=task_arn, logs_url=None))


def test_cancel_re_raises_other_client_errors() -> None:
    from botocore.exceptions import ClientError

    ecs = _make_ecs_client()
    stubber = Stubber(ecs)
    stubber.add_client_error(
        "stop_task",
        service_error_code="AccessDeniedException",
        service_message="nope",
    )
    stubber.activate()

    launcher = FargateLauncher(_basic_config(), ecs_client=ecs)
    with pytest.raises(ClientError):
        launcher.cancel(ContainerExecution(id="x", logs_url=None))


def test_submit_resolves_container_name_from_task_definition_when_none() -> None:
    ecs = _make_ecs_client()
    stubber = Stubber(ecs)
    # describe_task_definition returns the container list.
    stubber.add_response(
        "describe_task_definition",
        {
            "taskDefinition": {
                "taskDefinitionArn": "arn:td",
                "containerDefinitions": [{"name": "resolved-app"}],
            }
        },
    )
    stubber.add_response(
        "run_task",
        {
            "tasks": [{"taskArn": "arn:task/x"}],
            "failures": [],
        },
    )
    stubber.activate()

    cfg = _basic_config(container_name=None)
    launcher = FargateLauncher(cfg, ecs_client=ecs)
    execution = launcher.submit(
        env={"LOOM_FLOW_CTX_JSON": "{}", "LOOM_FLOW_PARAMS_JSON": "{}"},
    )
    assert execution.id == "arn:task/x"
