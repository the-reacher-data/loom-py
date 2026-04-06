"""Protocol for writing one execution record to a backend table."""

from __future__ import annotations

from typing import Protocol

from loom.etl.observability.records import ExecutionRecord
from loom.etl.schema._table import TableRef


class ExecutionRecordWriter(Protocol):
    """Persist one execution record into a table destination."""

    def write_record(self, record: ExecutionRecord, table_ref: TableRef, /) -> None:
        """Write *record* to *table_ref*."""


__all__ = ["ExecutionRecordWriter"]
