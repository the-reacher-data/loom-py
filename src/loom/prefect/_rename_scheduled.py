"""Rewrite Prefect's whimsical ``Scheduled`` run names to ``YYYY-MM-DDTHH:MM``."""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable
from uuid import UUID

_log = logging.getLogger(__name__)

_WHIMSICAL_NAME = re.compile(r"^[a-z]+-[a-z]+$")


def rename_scheduled_runs(deployment_ids: Iterable[str | UUID]) -> int:
    """Rename ``Scheduled`` flow runs of *deployment_ids* to date-based names.

    Args:
        deployment_ids: IDs returned by :func:`discover_and_deploy_etls`.

    Returns:
        Number of flow runs renamed.
    """
    ids = [UUID(str(d)) for d in deployment_ids]
    if not ids:
        return 0
    try:
        import asyncio  # noqa: PLC0415

        return asyncio.run(_rename_async(ids))
    except Exception:  # noqa: BLE001
        _log.warning("rename_scheduled_runs failed", exc_info=True)
        return 0


async def _rename_async(deployment_ids: list[UUID]) -> int:
    from prefect.client.orchestration import get_client  # noqa: PLC0415
    from prefect.client.schemas.filters import (  # noqa: PLC0415
        DeploymentFilter,
        DeploymentFilterId,
        FlowRunFilter,
        FlowRunFilterState,
        FlowRunFilterStateType,
    )
    from prefect.client.schemas.objects import StateType  # noqa: PLC0415

    renamed = 0
    async with get_client() as client:
        flow_runs = await client.read_flow_runs(
            deployment_filter=DeploymentFilter(
                id=DeploymentFilterId(any_=deployment_ids),
            ),
            flow_run_filter=FlowRunFilter(
                state=FlowRunFilterState(
                    type=FlowRunFilterStateType(any_=[StateType.SCHEDULED]),
                ),
            ),
        )
        for run in flow_runs:
            new_name = _desired_name(run)
            if new_name is None or run.name == new_name:
                continue
            # Skip operator-set names — only replace whimsical ones.
            if run.name and not _WHIMSICAL_NAME.match(run.name):
                continue
            try:
                await client.update_flow_run(flow_run_id=run.id, name=new_name)
                renamed += 1
            except Exception:  # noqa: BLE001
                _log.warning("failed to rename flow_run id=%s", run.id, exc_info=True)
    return renamed


def _desired_name(run: object) -> str | None:
    expected = getattr(run, "expected_start_time", None) or getattr(
        run, "scheduled_start_time", None
    )
    if expected is None:
        return None
    return str(expected.strftime("%Y-%m-%dT%H:%M"))


__all__ = ["rename_scheduled_runs"]
