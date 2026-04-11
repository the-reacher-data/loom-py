"""Table-based execution record store using backend writer adapters."""

from __future__ import annotations

from loom.etl.declarative.expr._refs import TableRef
from loom.etl.observability.records import (
    ExecutionRecord,
    get_record_table_name,
)
from loom.etl.observability.sinks._protocol import ExecutionRecordWriter


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
        try:
            table_name = get_record_table_name(type(record))
        except KeyError as exc:
            raise TypeError(
                f"TableExecutionRecordStore: unrecognised record type {type(record)!r}"
            ) from exc
        self._writer.write_record(record, _table_ref(self._database, table_name))


def _table_ref(database: str, table_name: str) -> TableRef:
    if database:
        return TableRef(f"{database}.{table_name}")
    return TableRef(table_name)


__all__ = ["TableExecutionRecordStore"]
