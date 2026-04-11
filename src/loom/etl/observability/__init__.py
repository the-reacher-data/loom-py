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
    RecordField,
    RunContext,
    RunStatus,
    StepRunRecord,
    get_record_table_name,
)
from loom.etl.observability.sinks import (
    ExecutionRecordStore,
    ExecutionRecordWriter,
    RecordFrameTargetWriter,
    TableExecutionRecordStore,
    TargetExecutionRecordWriter,
)

__all__ = [
    "CompositeObserver",
    "ETLRunObserver",
    "EventName",
    "ExecutionRecord",
    "ExecutionRecordStore",
    "ExecutionRecordWriter",
    "ExecutionRecordStoreConfig",
    "ExecutionRecordsObserver",
    "get_record_table_name",
    "NoopRunObserver",
    "ObservabilityConfig",
    "OtelConfig",
    "PipelineRunRecord",
    "ProcessRunRecord",
    "RecordField",
    "RecordFrameTargetWriter",
    "RunContext",
    "RunStatus",
    "StepRunRecord",
    "StructlogRunObserver",
    "TableExecutionRecordStore",
    "TargetExecutionRecordWriter",
    "make_observers",
]
