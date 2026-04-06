"""Backend-specific writers used by observability stores."""

from loom.etl.observability.writers.polars import PolarsExecutionRecordWriter
from loom.etl.observability.writers.protocol import ExecutionRecordWriter
from loom.etl.observability.writers.spark import SparkExecutionRecordWriter

__all__ = [
    "ExecutionRecordWriter",
    "PolarsExecutionRecordWriter",
    "SparkExecutionRecordWriter",
]
