"""AWS Fargate (ECS) implementation of :class:`ContainerLauncher`.

Wraps a ``boto3`` ECS client. Designed to be substitutable for a
unit-test stub via ``botocore.stub.Stubber``.
"""

from __future__ import annotations

from collections.abc import Mapping
from time import monotonic, sleep
from typing import Any

import boto3  # type: ignore[import-untyped]
import botocore.exceptions  # type: ignore[import-untyped]

from loom.core.model import LoomFrozenStruct
from loom.prefect._launcher import (
    ContainerExecution,
    ContainerLaunchError,
    ContainerResult,
    ContainerTaskTimeoutError,
)


class FargateConfig(LoomFrozenStruct, frozen=True, kw_only=True):
    """Static configuration for a :class:`FargateLauncher`.

    Args:
        cluster: ECS cluster name or ARN.
        task_definition: Task definition family[:revision].
        subnets: VPC subnets for the task ENI.
        security_groups: Security groups attached to the ENI.
        container_name: Name of the container inside the task definition
            that receives env overrides. ``None`` resolves lazily to the
            first container of the task definition on the first ``submit``.
        assign_public_ip: When ``True``, assigns a public IP to the task ENI.
        capacity_provider: Capacity provider name. ``None`` falls back to
            ``launchType=FARGATE``.
        etl_name: Logical ETL name. Auto-tagged as ``loom.etl=<etl_name>``.
    """

    cluster: str
    task_definition: str
    subnets: tuple[str, ...]
    security_groups: tuple[str, ...]
    container_name: str | None = None
    assign_public_ip: bool = False
    capacity_provider: str | None = None
    etl_name: str = ""


class FargateLauncher:
    """Submit, wait and cancel ECS tasks on AWS Fargate.

    Args:
        config: Static :class:`FargateConfig`.
        ecs_client: Optional preconfigured boto3 ECS client. If ``None``,
            a default client is created. Tests inject a stubbed client.
    """

    def __init__(
        self,
        config: FargateConfig,
        *,
        ecs_client: Any | None = None,
    ) -> None:
        self._cfg = config
        self._ecs_client: Any | None = ecs_client
        self._resolved_container_name: str | None = config.container_name

    @property
    def _ecs(self) -> Any:
        if self._ecs_client is None:
            self._ecs_client = boto3.client("ecs")
        return self._ecs_client

    # ------------------------------------------------------------------ submit
    def submit(
        self,
        *,
        env: Mapping[str, str],
        tags: Mapping[str, str] | None = None,
    ) -> ContainerExecution:
        """Call ``ecs.run_task`` with derived overrides and tags."""
        extra_tags: Mapping[str, str] = tags or {}
        container_name = self._resolve_container_name()
        environment = [{"name": key, "value": value} for key, value in env.items()]
        all_tags: list[dict[str, str]] = [{"key": "loom.etl", "value": self._cfg.etl_name}]
        for key, value in extra_tags.items():
            all_tags.append({"key": key, "value": value})

        params: dict[str, Any] = {
            "cluster": self._cfg.cluster,
            "taskDefinition": self._cfg.task_definition,
            "networkConfiguration": {
                "awsvpcConfiguration": {
                    "subnets": list(self._cfg.subnets),
                    "securityGroups": list(self._cfg.security_groups),
                    "assignPublicIp": ("ENABLED" if self._cfg.assign_public_ip else "DISABLED"),
                }
            },
            "overrides": {
                "containerOverrides": [{"name": container_name, "environment": environment}]
            },
            "tags": all_tags,
        }
        if self._cfg.capacity_provider is None:
            params["launchType"] = "FARGATE"
        else:
            params["capacityProviderStrategy"] = [
                {"capacityProvider": self._cfg.capacity_provider, "weight": 1}
            ]

        response = self._ecs.run_task(**params)
        failures = response.get("failures") or []
        if failures:
            raise ContainerLaunchError(f"ecs.run_task failed: {failures!r}")
        tasks = response.get("tasks") or []
        if not tasks:
            raise ContainerLaunchError("ecs.run_task returned no tasks")
        task_arn = tasks[0]["taskArn"]
        return ContainerExecution(id=task_arn, logs_url=None)

    # -------------------------------------------------------------------- wait
    def wait(
        self,
        execution: ContainerExecution,
        *,
        poll_interval_seconds: float,
        timeout_seconds: int,
    ) -> ContainerResult:
        """Poll ``describe_tasks`` until STOPPED or until the timeout fires."""
        container_name = self._resolve_container_name()
        start = monotonic()
        while monotonic() - start < timeout_seconds:
            response = self._ecs.describe_tasks(cluster=self._cfg.cluster, tasks=[execution.id])
            tasks = response.get("tasks") or []
            if not tasks:
                raise ContainerLaunchError(f"task {execution.id} not found in describe_tasks")
            task = tasks[0]
            if task.get("lastStatus") == "STOPPED":
                containers = task.get("containers") or []
                target = next(
                    (c for c in containers if c.get("name") == container_name),
                    None,
                )
                exit_code_raw: Any = target.get("exitCode") if target is not None else None
                exit_code = int(exit_code_raw) if exit_code_raw is not None else -1
                return ContainerResult(
                    exit_code=exit_code,
                    reason=task.get("stoppedReason"),
                )
            sleep(poll_interval_seconds)
        raise ContainerTaskTimeoutError(execution_id=execution.id)

    # ------------------------------------------------------------------ cancel
    def cancel(self, execution: ContainerExecution) -> None:
        """Best-effort ``stop_task``. Idempotent on ``InvalidParameterException``."""
        try:
            self._ecs.stop_task(
                cluster=self._cfg.cluster,
                task=execution.id,
                reason="loom: timeout cancel",
            )
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code != "InvalidParameterException":
                raise

    # ----------------------------------------------------------------- helpers
    def _resolve_container_name(self) -> str:
        if self._resolved_container_name is not None:
            return self._resolved_container_name
        td = self._ecs.describe_task_definition(taskDefinition=self._cfg.task_definition)
        containers = td["taskDefinition"]["containerDefinitions"]
        if not containers:
            raise ContainerLaunchError(
                f"task definition {self._cfg.task_definition} has no containers"
            )
        resolved: str = containers[0]["name"]
        self._resolved_container_name = resolved
        return resolved


__all__ = ["FargateConfig", "FargateLauncher"]
