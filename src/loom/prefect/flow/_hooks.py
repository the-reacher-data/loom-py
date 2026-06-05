"""Prefect lifecycle hooks attached to loom ETL flows."""

from __future__ import annotations

import logging
from typing import Any

from loom.prefect._async import run_sync

_log = logging.getLogger(__name__)


def pause_schedule_on_failure(flow: Any, flow_run: Any, state: Any) -> None:
    """Prefect ``on_failure`` hook: deactivate the deployment's schedules.

    Fires only when the flow finally enters Failed state (after
    ``flow_retries`` have been exhausted). Stops the cron from firing
    further until an operator investigates and re-enables the schedule.

    Best-effort: any error here is logged and swallowed — pausing the
    schedule is a safety net, not part of the critical path.

    Args:
        flow: The Prefect ``Flow`` instance (unused; required by Prefect's
            hook signature).
        flow_run: The Prefect ``FlowRun`` state container. Reads
            ``deployment_id`` from it.
        state: The terminal Prefect state (unused; required by Prefect's
            hook signature).
    """
    deployment_id = getattr(flow_run, "deployment_id", None)
    if not deployment_id:
        return  # ad-hoc run, no schedule to pause

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


__all__ = ["pause_schedule_on_failure"]
