"""Polars-backed execution record writer."""

from __future__ import annotations

from typing import Any

from loom.etl.observability.records import ExecutionRecord
from loom.etl.observability.writers._row import record_to_row
from loom.etl.schema._table import TableRef


class PolarsExecutionRecordWriter:
    """Write execution records through the Polars target writer ``append`` API."""

    def __init__(self, writer: Any) -> None:
        self._writer = writer

    def write_record(self, record: ExecutionRecord, table_ref: TableRef, /) -> None:
        """Append one execution record row to *table_ref*."""
        import polars as pl

        frame = pl.DataFrame([record_to_row(record)]).lazy()
        self._writer.append(frame, table_ref, None)


__all__ = ["PolarsExecutionRecordWriter"]
