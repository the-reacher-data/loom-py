"""Shared row conversion helpers for execution records."""

from __future__ import annotations

import dataclasses
from typing import Any

from loom.etl.observability.records import ExecutionRecord


def record_to_row(record: ExecutionRecord) -> dict[str, Any]:
    """Convert an execution record dataclass into a plain row mapping."""
    row = dataclasses.asdict(record)
    row["event"] = str(row["event"])
    row["status"] = str(row["status"])
    return row


__all__ = ["record_to_row"]
