"""Observability public API for ETL runtime hooks and persisted records."""

from loom.etl.observability._polars_writer import PolarsExecutionRecordWriter
from loom.etl.observability._spark_writer import SparkExecutionRecordWriter
from loom.etl.observability.config import ExecutionRecordStoreConfig, ObservabilityConfig
from loom.etl.observability.factory import make_observers
from loom.etl.observability.observers import (
    CompositeObserver,
    ETLRunObserver,
    ExecutionRecordsObserver,
    NoopRunObserver,
    StructlogRunObserver,
)
from loom.etl.observability.records import (
    EventName,
    ExecutionRecord,
    PipelineRunRecord,
    ProcessRunRecord,
    RunContext,
    RunStatus,
    StepRunRecord,
)
from loom.etl.observability.stores import (
    ExecutionRecordStore,
    ExecutionRecordWriter,
    TableExecutionRecordStore,
)

__all__ = [
    "CompositeObserver",
    "ETLRunObserver",
    "EventName",
    "ExecutionRecord",
    "ExecutionRecordStore",
    "ExecutionRecordStoreConfig",
    "ExecutionRecordWriter",
    "ExecutionRecordsObserver",
    "NoopRunObserver",
    "ObservabilityConfig",
    "PipelineRunRecord",
    "PolarsExecutionRecordWriter",
    "ProcessRunRecord",
    "RunContext",
    "RunStatus",
    "SparkExecutionRecordWriter",
    "StepRunRecord",
    "StructlogRunObserver",
    "TableExecutionRecordStore",
    "make_observers",
]
