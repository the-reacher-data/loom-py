"""Generic lineage-record writer backed by a target writer."""

from __future__ import annotations

from loom.etl.declarative.expr._refs import TableRef
from loom.etl.lineage._records import LineageRecord
from loom.etl.lineage.sinks._protocol import RecordFrameTargetWriter


class TargetLineageWriter:
    """Persist lineage records using target writer ``to_frame`` + ``append``."""

    def __init__(self, writer: RecordFrameTargetWriter) -> None:
        self._writer = writer

    def write_record(self, record: LineageRecord, table_ref: TableRef, /) -> None:
        """Append one lineage record row into *table_ref*."""
        frame = self._writer.to_frame([record])
        self._writer.append(frame, table_ref, None)


__all__ = ["TargetLineageWriter"]
