"""Persistence contracts for execution records."""

from __future__ import annotations

from typing import Protocol

from loom.etl.observability.records import ExecutionRecord
from loom.etl.schema._table import TableRef


class ExecutionRecordStore(Protocol):
    """Protocol for persisting execution records."""

    def write_record(self, record: ExecutionRecord) -> None:
        """Persist one completed execution record."""


class ExecutionRecordWriter(Protocol):
    """Persist one execution record into a table destination."""

    def write_record(self, record: ExecutionRecord, table_ref: TableRef, /) -> None:
        """Write *record* to *table_ref*."""


__all__ = ["ExecutionRecordStore", "ExecutionRecordWriter"]
