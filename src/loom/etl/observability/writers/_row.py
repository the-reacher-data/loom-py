"""Shared row conversion helpers for execution records."""

from __future__ import annotations

import dataclasses
from typing import Any

from loom.etl.observability.records import ExecutionRecord


def record_to_row(record: ExecutionRecord) -> dict[str, Any]:
    """Convert an execution record dataclass into a plain row mapping."""
    row = dataclasses.asdict(record)
    # Persist only snapshot fields in Delta tables; lifecycle event type is
    # still used by log observers but does not add analytical value here.
    row.pop("event", None)
    row["status"] = str(row["status"])
    return row


__all__ = ["record_to_row"]
