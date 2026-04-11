"""Observability public API for ETL runtime hooks and persisted records."""

from loom.etl.observability.config import (
    ExecutionRecordStoreConfig,
    ObservabilityConfig,
    OtelConfig,
)
from loom.etl.observability.factory import make_observers
from loom.etl.observability.observers import (
    CompositeObserver,
    ETLRunObserver,
    NoopRunObserver,
    StructlogRunObserver,
)
from loom.etl.observability.recording import ExecutionRecordsObserver
from loom.etl.observability.records import (
    EventName,
    ExecutionRecord,
    PipelineRunRecord,
    ProcessRunRecord,
    RunContext,
    RunStatus,
    StepRunRecord,
)
from loom.etl.observability.sinks import (
    ExecutionRecordStore,
    PolarsExecutionRecordWriter,
    SparkExecutionRecordWriter,
    TableExecutionRecordStore,
)

__all__ = [
    "CompositeObserver",
    "ETLRunObserver",
    "EventName",
    "ExecutionRecord",
    "ExecutionRecordStore",
    "ExecutionRecordStoreConfig",
    "ExecutionRecordsObserver",
    "NoopRunObserver",
    "ObservabilityConfig",
    "OtelConfig",
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
