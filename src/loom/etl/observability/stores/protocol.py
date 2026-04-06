"""Persistence contract for execution records."""

from __future__ import annotations

from typing import Protocol

from loom.etl.observability.records import ExecutionRecord


class ExecutionRecordStore(Protocol):
    """Protocol for persisting execution records."""

    def write_record(self, record: ExecutionRecord) -> None:
        """Persist one completed execution record."""


__all__ = ["ExecutionRecordStore"]
