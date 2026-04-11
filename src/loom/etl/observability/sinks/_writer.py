"""Generic execution-record writer backed by a target writer."""

from __future__ import annotations

from loom.etl.declarative.expr._refs import TableRef
from loom.etl.observability.records import ExecutionRecord
from loom.etl.observability.sinks._protocol import RecordFrameTargetWriter


class TargetExecutionRecordWriter:
    """Persist execution records using target writer ``to_frame`` + ``append``.

    Args:
        writer: Backend target writer exposing ``to_frame`` and ``append``.
    """

    def __init__(self, writer: RecordFrameTargetWriter) -> None:
        self._writer = writer

    def write_record(self, record: ExecutionRecord, table_ref: TableRef, /) -> None:
        """Append one execution record row into *table_ref*."""
        frame = self._writer.to_frame([record])
        self._writer.append(frame, table_ref, None)


__all__ = ["TargetExecutionRecordWriter"]
