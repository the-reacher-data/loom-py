"""Container launcher abstraction and the ``run_etl_in_container`` task.

Defines the :class:`ContainerLauncher` Protocol that concrete backends
(Fargate, local Docker, future Kubernetes) must satisfy, the value
objects exchanged with it (:class:`ContainerExecution`,
:class:`ContainerResult`), the typed error hierarchy, and the Prefect
task that drives the full submit / wait / cancel lifecycle.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable

import msgspec
import prefect

from loom.core.model import LoomFrozenStruct

_logger = logging.getLogger(__name__)


class ContainerExecution(LoomFrozenStruct, frozen=True, kw_only=True):
    """Handle for an in-flight container execution.

    Args:
        id: Backend identifier (ECS task ARN or Docker container id).
        logs_url: Best-effort URL pointing to the live logs, or ``None`` if
            the launcher cannot construct one.
    """

    id: str
    logs_url: str | None


class ContainerResult(LoomFrozenStruct, frozen=True, kw_only=True):
    """Terminal result of a container execution.

    Args:
        exit_code: Process exit code reported by the container. ``-1`` when
            the backend could not report one (e.g. ECS task failed to start).
        reason: Backend-specific stop reason, or ``None`` if the container
            finished cleanly.
    """

    exit_code: int
    reason: str | None


class ContainerLaunchError(RuntimeError):
    """Raised when ``submit()`` itself fails or the backend reports failures."""


class ContainerTaskFailedError(RuntimeError):
    """Raised when a container finished with a non-zero exit code.

    Attributes:
        execution_id: Backend identifier of the failed execution.
        exit_code: Reported exit code.
        reason: Backend-specific reason, if any.
    """

    def __init__(
        self,
        *,
        execution_id: str,
        exit_code: int,
        reason: str | None,
    ) -> None:
        super().__init__(f"container {execution_id} exited with {exit_code} ({reason})")
        self.execution_id = execution_id
        self.exit_code = exit_code
        self.reason = reason


class ContainerTaskTimeoutError(TimeoutError):
    """Raised when a container exceeds the configured timeout.

    Attributes:
        execution_id: Backend identifier of the timed-out execution.
    """

    def __init__(self, *, execution_id: str) -> None:
        super().__init__(f"container {execution_id} timed out")
        self.execution_id = execution_id


@runtime_checkable
class ContainerLauncher(Protocol):
    """Submit, wait and cancel external container executions.

    Implementations translate generic environment + tag inputs into
    backend-specific calls (ECS, Docker, ...). ``tags`` semantics:

    - Fargate: translated to ECS task tags
      (``[{"key": k, "value": v}, ...]``).
    - Docker: translated to container labels (``{k: v}``).

    Implementations are responsible for auto-injecting ``loom.etl=<name>``
    on top of any user-provided tags. Tag keys/values must respect ECS
    charset rules (``[a-zA-Z0-9 _.:/=+\\-@]``); the launcher does not
    sanitize them.
    """

    def submit(
        self,
        *,
        env: Mapping[str, str],
        tags: Mapping[str, str] | None = None,
    ) -> ContainerExecution:
        """Start a container and return a handle to it.

        ``tags`` may be ``None`` (treated as no extra tags). Implementations
        must avoid mutating the mapping.
        """
        ...

    def wait(
        self,
        execution: ContainerExecution,
        *,
        poll_interval_seconds: float,
        timeout_seconds: int,
    ) -> ContainerResult:
        """Block until the container reaches a terminal state or the timeout fires."""
        ...

    def cancel(self, execution: ContainerExecution) -> None:
        """Best-effort cancellation. Must be idempotent."""
        ...


@prefect.task(name="run-etl-in-container")
def run_etl_in_container(
    *,
    ctx: Any,
    params: msgspec.Struct,
    launcher: ContainerLauncher,
    flow_name: str | None = None,
    poll_interval_seconds: float = 15.0,
    timeout_seconds: int = 3600,
) -> int:
    """Submit, wait, and surface the terminal state of an ETL container.

    Serializes ``ctx`` and ``params`` as JSON into two environment variables
    (``LOOM_FLOW_CTX_JSON`` and ``LOOM_FLOW_PARAMS_JSON``), injects three
    standard tags (``loom.correlation_id``, ``loom.run_id``,
    ``loom.environment``), and delegates the actual execution to the
    provided launcher.

    When ``flow_name`` is provided, also exports ``LOOM_FLOW_NAME`` so the
    container's entrypoint can name its inner ``build_etl_flow`` with the
    same name as the outer launcher flow. Both flow runs then collapse to a
    single entry in Prefect's Flows catalog, distinguishable per cron tick
    by the ``correlation_id`` tag and by whether they are bound to a
    Deployment.

    Args:
        ctx: Operational :class:`FlowCtx` (correlation_id, run_id,
            environment, ...).
        params: ``msgspec.Struct`` instance carrying the ETL parameters.
        launcher: Backend implementing :class:`ContainerLauncher`.
        flow_name: Name to use for the inner flow inside the container.
            Typically the outer ETL name (e.g. ``"daily-orders"``).
            ``None`` skips the ``LOOM_FLOW_NAME`` export; the container's
            inner flow falls back to whatever name its own code chooses.
        poll_interval_seconds: Sleep between polls inside ``wait``.
        timeout_seconds: Maximum lifetime for the execution.

    Returns:
        ``0`` on clean exit.

    Raises:
        ContainerTaskFailedError: Container exited with a non-zero status.
        ContainerTaskTimeoutError: Timeout reached before the container
            finished; the launcher's ``cancel()`` is invoked best-effort
            beforehand and any error from it is swallowed.
        ContainerLaunchError: ``submit()`` raised; nothing to cancel.
    """
    env: dict[str, str] = {
        "LOOM_FLOW_CTX_JSON": msgspec.json.encode(ctx).decode(),
        "LOOM_FLOW_PARAMS_JSON": msgspec.json.encode(params).decode(),
    }
    if flow_name is not None:
        env["LOOM_FLOW_NAME"] = flow_name
    tags: dict[str, str] = {
        "loom.correlation_id": ctx.correlation_id,
        "loom.run_id": ctx.run_id,
        "loom.environment": ctx.environment,
    }
    execution = launcher.submit(env=env, tags=tags)
    _logger.info("container launched: %s", execution.id)
    try:
        result = launcher.wait(
            execution,
            poll_interval_seconds=poll_interval_seconds,
            timeout_seconds=timeout_seconds,
        )
    except ContainerTaskTimeoutError:
        try:
            launcher.cancel(execution)
        except Exception:  # noqa: BLE001 — cancel is best-effort.
            _logger.warning(
                "cancel() raised for %s after timeout; swallowing",
                execution.id,
                exc_info=True,
            )
        raise
    if result.exit_code != 0:
        raise ContainerTaskFailedError(
            execution_id=execution.id,
            exit_code=result.exit_code,
            reason=result.reason,
        )
    return 0


__all__ = [
    "ContainerExecution",
    "ContainerLaunchError",
    "ContainerLauncher",
    "ContainerResult",
    "ContainerTaskFailedError",
    "ContainerTaskTimeoutError",
    "run_etl_in_container",
]
