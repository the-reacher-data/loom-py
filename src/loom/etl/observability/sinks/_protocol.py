"""Persistence contracts for execution records."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

from loom.etl.declarative.expr._refs import TableRef
from loom.etl.observability.records import ExecutionRecord


class ExecutionRecordStore(Protocol):
    """Protocol for persisting execution records."""

    def write_record(self, record: ExecutionRecord) -> None:
        """Persist one completed execution record."""


class ExecutionRecordWriter(Protocol):
    """Persist one execution record into a table destination."""

    def write_record(self, record: ExecutionRecord, table_ref: TableRef, /) -> None:
        """Write *record* to *table_ref*."""


class RecordFrameTargetWriter(Protocol):
    """Target-writer capability required to persist execution records."""

    def to_frame(self, records: Sequence[ExecutionRecord], /) -> Any:
        """Convert execution records into backend frame type."""

    def append(
        self,
        frame: Any,
        table_ref: TableRef,
        params_instance: Any,
        /,
        *,
        streaming: bool = False,
    ) -> None:
        """Append backend frame into destination table."""


__all__ = ["ExecutionRecordStore", "ExecutionRecordWriter", "RecordFrameTargetWriter"]
