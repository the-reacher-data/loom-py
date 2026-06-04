"""Local Docker implementation of :class:`ContainerLauncher`.

Wraps the official ``docker`` Python SDK. Designed for the PoC workflow
where the Prefect worker spawns containers via the host's docker socket.
"""

from __future__ import annotations

import contextlib
from collections.abc import Mapping
from typing import Any

import docker
import docker.errors
import requests.exceptions

from loom.core.model import LoomFrozenStruct
from loom.prefect._launcher import (
    ContainerExecution,
    ContainerResult,
    ContainerTaskTimeoutError,
)


class DockerConfig(LoomFrozenStruct, frozen=True, kw_only=True):
    """Static configuration for a :class:`LocalDockerLauncher`.

    Args:
        image: Docker image reference (with tag).
        network: Docker network the container joins, or ``None``.
        cpu: CPU shares as a decimal string ("2", "0.5"). Translated to
            ``nano_cpus = int(float(cpu) * 1e9)``. ``None`` skips the limit.
        memory: Memory limit string passed verbatim as ``mem_limit``
            (e.g. ``"1g"``). ``None`` skips the limit.
        volumes: Tuple of ``(host_path, container_path)`` bind mounts.
        etl_name: Logical ETL name. Auto-labeled as ``loom.etl=<etl_name>``.
    """

    image: str
    network: str | None = None
    cpu: str | None = None
    memory: str | None = None
    volumes: tuple[tuple[str, str], ...] = ()
    etl_name: str = ""


class LocalDockerLauncher:
    """Submit, wait and cancel containers via the local docker daemon.

    Args:
        config: Static :class:`DockerConfig`.
        docker_client: Optional preconfigured docker client. ``None``
            uses :func:`docker.from_env`. Tests inject a ``MagicMock``.
    """

    def __init__(
        self,
        config: DockerConfig,
        *,
        docker_client: Any | None = None,
    ) -> None:
        self._cfg = config
        self._client = docker_client if docker_client is not None else docker.from_env()

    # ------------------------------------------------------------------ submit
    def submit(
        self,
        *,
        env: Mapping[str, str],
        tags: Mapping[str, str] | None = None,
    ) -> ContainerExecution:
        """Call ``client.containers.run(..., detach=True)``."""
        extra_tags: Mapping[str, str] = tags or {}
        labels: dict[str, str] = {"loom.etl": self._cfg.etl_name, **dict(extra_tags)}
        kwargs: dict[str, Any] = {
            "image": self._cfg.image,
            "environment": dict(env),
            "detach": True,
            "network": self._cfg.network,
            "labels": labels,
        }
        if self._cfg.cpu is not None:
            kwargs["nano_cpus"] = int(float(self._cfg.cpu) * 1e9)
        if self._cfg.memory is not None:
            kwargs["mem_limit"] = self._cfg.memory
        if self._cfg.volumes:
            kwargs["volumes"] = {
                host: {"bind": container, "mode": "rw"} for host, container in self._cfg.volumes
            }

        container = self._client.containers.run(**kwargs)
        return ContainerExecution(id=container.id, logs_url=None)

    # -------------------------------------------------------------------- wait
    def wait(
        self,
        execution: ContainerExecution,
        *,
        poll_interval_seconds: float,
        timeout_seconds: int,
    ) -> ContainerResult:
        """Block on ``container.wait`` and translate timeouts."""
        container = self._client.containers.get(execution.id)
        try:
            result = container.wait(timeout=timeout_seconds)
        except requests.exceptions.ReadTimeout:
            raise ContainerTaskTimeoutError(execution_id=execution.id) from None
        try:
            container.reload()
            error = container.attrs.get("State", {}).get("Error") or None
        except (docker.errors.APIError, docker.errors.NotFound):
            error = None
        return ContainerResult(
            exit_code=int(result["StatusCode"]),
            reason=error,
        )

    # ------------------------------------------------------------------ cancel
    def cancel(self, execution: ContainerExecution) -> None:
        """Best-effort ``kill``. Idempotent on NotFound / APIError."""
        with contextlib.suppress(docker.errors.NotFound, docker.errors.APIError):
            self._client.containers.get(execution.id).kill()


__all__ = ["DockerConfig", "LocalDockerLauncher"]
