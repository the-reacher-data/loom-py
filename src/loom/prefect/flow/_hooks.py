"""Prefect lifecycle hooks attached to loom ETL flows."""

from __future__ import annotations

import logging
import os
from collections.abc import Iterable
from typing import Any

from loom.prefect._async import run_sync
from loom.prefect.notify import Notifier, NotifyEvent

_log = logging.getLogger(__name__)


def pause_schedule_on_failure(flow: Any, flow_run: Any, state: Any) -> None:  # noqa: ARG001
    """Deactivate the deployment's schedules on terminal failure."""
    del flow, state  # NOSONAR S1172 - signature dictated by Prefect's FlowStateHook protocol
    deployment_id = getattr(flow_run, "deployment_id", None)
    if not deployment_id:
        return

    try:
        from prefect.client.orchestration import get_client  # noqa: PLC0415

        async def _pause() -> None:
            async with get_client() as client:
                deployment = await client.read_deployment(deployment_id)
                for sched in deployment.schedules or []:
                    await client.update_deployment_schedule(deployment_id, sched.id, active=False)

        run_sync(_pause())
        _log.warning(
            "pause-on-failure: deactivated schedules for deployment %s",
            deployment_id,
        )
    except Exception:  # noqa: BLE001
        _log.warning("pause-on-failure hook failed", exc_info=True)


def make_notification_hooks(
    flow_name: str,
    notifiers: Iterable[Notifier],
) -> tuple[list[Any], list[Any]]:
    """Return Prefect ``on_failure`` / ``on_completion`` hook lists."""
    notifiers_tuple = tuple(notifiers)
    if not notifiers_tuple:
        return [], []

    def _dispatch(state_name: str) -> Any:
        def _hook(flow: Any, flow_run: Any, state: Any) -> None:  # noqa: ARG001
            del flow  # NOSONAR S1172 - signature dictated by Prefect's FlowStateHook protocol
            event = _event_from_run(flow_name, flow_run, state, state_name)
            for n in notifiers_tuple:
                try:
                    n.notify(event)
                except Exception:  # noqa: BLE001
                    _log.warning("notifier raised — swallowed", exc_info=True)

        return _hook

    return [_dispatch("Failed")], [_dispatch("Completed")]


def _event_from_run(
    flow_name: str,
    flow_run: Any,
    state: Any,
    state_name: str,
) -> NotifyEvent:
    return NotifyEvent(
        flow_name=flow_name,
        flow_run_name=getattr(flow_run, "name", "") or "",
        flow_run_url=_run_url(flow_run),
        state=state_name,
        correlation_id=_correlation_from_params(flow_run),
        message=getattr(state, "message", None),
    )


def _run_url(flow_run: Any) -> str:
    base = os.environ.get("PREFECT_UI_URL") or os.environ.get("PREFECT_API_URL", "")
    base = base.rstrip("/").removesuffix("/api")
    run_id = getattr(flow_run, "id", None)
    return f"{base}/flow-runs/flow-run/{run_id}" if base and run_id else ""


def _correlation_from_params(flow_run: Any) -> str:
    params = getattr(flow_run, "parameters", None) or {}
    return str(params.get("correlation_id") or "")


__all__ = ["make_notification_hooks", "pause_schedule_on_failure"]
