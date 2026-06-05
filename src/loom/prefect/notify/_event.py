"""Frozen payload that every Notifier receives."""

from __future__ import annotations

import msgspec


class NotifyEvent(msgspec.Struct, frozen=True, kw_only=True):
    """Lifecycle snapshot sent to notifiers on terminal flow states.

    Args:
        flow_name: Logical ETL name (``meta.name``).
        flow_run_name: Computed run name (e.g. correlation_id + HHMMSS).
        flow_run_url: Direct link to the flow run in the Prefect UI.
        state: Terminal Prefect state name (``"Completed"``, ``"Failed"``,
            ``"Crashed"``).
        correlation_id: Business correlation id of the run.
        message: Optional human-readable error or status message.
    """

    flow_name: str
    flow_run_name: str
    flow_run_url: str
    state: str
    correlation_id: str
    message: str | None = None


__all__ = ["NotifyEvent"]
