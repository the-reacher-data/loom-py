"""Sinks for execution record persistence."""

from loom.etl.observability.sinks._polars import PolarsExecutionRecordWriter
from loom.etl.observability.sinks._protocol import ExecutionRecordStore
from loom.etl.observability.sinks._spark import SparkExecutionRecordWriter
from loom.etl.observability.sinks._table import TableExecutionRecordStore

__all__ = [
    "ExecutionRecordStore",
    "PolarsExecutionRecordWriter",
    "SparkExecutionRecordWriter",
    "TableExecutionRecordStore",
]
