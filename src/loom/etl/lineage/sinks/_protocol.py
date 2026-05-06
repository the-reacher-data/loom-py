"""Persistence contracts for lineage records."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

from loom.etl.declarative.expr._refs import TableRef
from loom.etl.lineage._records import LineageRecord


class LineageStore(Protocol):
    """Protocol for persisting lineage records."""

    def write_record(self, record: LineageRecord) -> None:
        """Persist one completed lineage record."""


class LineageWriter(Protocol):
    """Persist one lineage record into a table destination."""

    def write_record(self, record: LineageRecord, table_ref: TableRef, /) -> None:
        """Write *record* to *table_ref*."""


class RecordFrameTargetWriter(Protocol):
    """Target-writer capability required to persist lineage records."""

    def to_frame(self, records: Sequence[LineageRecord], /) -> Any:
        """Convert lineage records into backend frame type."""

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


__all__ = ["LineageStore", "LineageWriter", "RecordFrameTargetWriter"]
