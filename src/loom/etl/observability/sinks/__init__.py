"""Sinks for execution record persistence."""

from loom.etl.observability.sinks._protocol import (
    ExecutionRecordStore,
    ExecutionRecordWriter,
    RecordFrameTargetWriter,
)
from loom.etl.observability.sinks._table import TableExecutionRecordStore
from loom.etl.observability.sinks._writer import TargetExecutionRecordWriter

__all__ = [
    "ExecutionRecordStore",
    "ExecutionRecordWriter",
    "RecordFrameTargetWriter",
    "TableExecutionRecordStore",
    "TargetExecutionRecordWriter",
]
