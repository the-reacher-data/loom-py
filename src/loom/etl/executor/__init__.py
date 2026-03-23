"""ETL executor public API."""

from loom.etl.executor._dispatcher import ParallelDispatcher, ThreadDispatcher
from loom.etl.executor._executor import ETLExecutor
from loom.etl.executor.observer import (
    ETLRunObserver,
    EventName,
    PipelineRunRecord,
    ProcessRunRecord,
    RunRecord,
    RunSink,
    RunSinkObserver,
    RunStatus,
    StepRunRecord,
    StructlogRunObserver,
)

__all__ = [
    # executor
    "ETLExecutor",
    # observer protocol
    "ETLRunObserver",
    # run sink protocol
    "RunSink",
    # events and records
    "EventName",
    "RunStatus",
    "RunRecord",
    "PipelineRunRecord",
    "ProcessRunRecord",
    "StepRunRecord",
    # implementations
    "StructlogRunObserver",
    "RunSinkObserver",
    # dispatcher protocol + implementations
    "ParallelDispatcher",
    "ThreadDispatcher",
]
