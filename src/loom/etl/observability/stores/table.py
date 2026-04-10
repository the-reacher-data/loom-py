"""Table-based execution record store using backend writer adapters."""

from __future__ import annotations

from typing import Any

from loom.etl.declarative.expr._refs import TableRef
from loom.etl.observability.records import (
    ExecutionRecord,
    PipelineRunRecord,
    ProcessRunRecord,
    StepRunRecord,
)
from loom.etl.observability.stores.protocol import ExecutionRecordWriter

_TABLE_FOR: dict[type[Any], str] = {
    PipelineRunRecord: "pipeline_runs",
    ProcessRunRecord: "process_runs",
    StepRunRecord: "step_runs",
}


class TableExecutionRecordStore:
    """Persist execution records into backend tables.

    Args:
        writer: Backend-aware execution record writer.
        database: Optional database/schema prefix for table names.
    """

    def __init__(self, writer: ExecutionRecordWriter, *, database: str = "") -> None:
        self._writer = writer
        self._database = database.strip()

    def write_record(self, record: ExecutionRecord) -> None:
        """Append *record* to the corresponding table."""
        table_name = _TABLE_FOR.get(type(record))
        if table_name is None:
            raise TypeError(f"TableExecutionRecordStore: unrecognised record type {type(record)!r}")
        self._writer.write_record(record, _table_ref(self._database, table_name))


def _table_ref(database: str, table_name: str) -> TableRef:
    if database:
        return TableRef(f"{database}.{table_name}")
    return TableRef(table_name)


__all__ = ["TableExecutionRecordStore"]
