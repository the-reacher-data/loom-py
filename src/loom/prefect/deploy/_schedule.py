"""Schedule + work-pool configuration helpers."""

from __future__ import annotations

from typing import Any


def extract_pool_config(raw_cfg: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Return per-environment work-pool overrides from the per-ETL YAML.

    Args:
        raw_cfg: Top-level YAML mapping returned by ``read_yaml``.

    Returns:
        Mapping ``environment → {"work_pool": str | None,
        "job_variables": dict}``. Empty when the YAML has no
        ``environments`` block.
    """
    environments = raw_cfg.get("environments") or {}
    out: dict[str, dict[str, Any]] = {}
    for env_name, env_block in environments.items():
        if not isinstance(env_block, dict):
            continue
        out[env_name] = {
            "work_pool": env_block.get("work_pool"),
            "job_variables": env_block.get("job_variables", {}) or {},
        }
    return out


def build_cron_schedule(schedule: dict[str, Any] | None) -> Any | None:
    """Build a Prefect ``DeploymentScheduleCreate`` from the YAML's schedule.

    Args:
        schedule: The ``schedule`` block from the per-ETL YAML, or
            ``None`` when the ETL is ad-hoc.

    Returns:
        A ``DeploymentScheduleCreate`` ready to pass as
        ``schedules=[...]`` to ``deploy(...)``, or ``None`` when the
        block disables scheduling.
    """
    if schedule is None or not schedule.get("enabled", True):
        return None
    cron = schedule.get("cron")
    if cron is None:
        return None
    from prefect.client.schemas.actions import (  # noqa: PLC0415
        DeploymentScheduleCreate,
    )
    from prefect.client.schemas.schedules import CronSchedule  # noqa: PLC0415

    return DeploymentScheduleCreate(
        schedule=CronSchedule(cron=cron, timezone=schedule.get("timezone")),
        active=True,
        max_scheduled_runs=int(schedule.get("max_scheduled_runs", 1)),
    )


__all__ = ["build_cron_schedule", "extract_pool_config"]
