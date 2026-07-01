"""Shared helpers for Prefect flow factories (etl_flow, maintenance_flow, …)."""

from __future__ import annotations

import uuid
from typing import Any, cast


def coerce_tags(raw: Any) -> tuple[str, ...]:
    """Parse the ``tags`` list from a raw YAML value into a tuple of strings."""
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise TypeError(f"tags: expected a list of strings, got {type(raw).__name__}")
    for value in raw:
        if not isinstance(value, str):
            raise TypeError(f"tags: every entry must be str, got {type(value).__name__}")
    return tuple(raw)


def prefect_flow_run_id() -> uuid.UUID | None:
    """Return the current Prefect flow run identifier.

    Returns:
        The active flow run UUID, or ``None`` outside a Prefect flow run.
    """
    try:
        from prefect.runtime import flow_run  # noqa: PLC0415

        return cast(uuid.UUID | None, flow_run.id)
    except (ImportError, AttributeError):
        return None


__all__ = ["coerce_tags", "prefect_flow_run_id"]
