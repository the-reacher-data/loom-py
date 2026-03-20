"""ETL executor public API."""

from loom.etl.executor._dispatcher import ParallelDispatcher, ThreadDispatcher
from loom.etl.executor._executor import ETLExecutor
from loom.etl.executor._observer import (
    CompositeRunObserver,
    ETLRunObserver,
    EventName,
    LoggingRunObserver,
    NoopRunObserver,
    RunStatus,
)

__all__ = [
    # executor
    "ETLExecutor",
    # observer protocol + implementations
    "ETLRunObserver",
    "RunStatus",
    "EventName",
    "NoopRunObserver",
    "LoggingRunObserver",
    "CompositeRunObserver",
    # dispatcher protocol + implementations
    "ParallelDispatcher",
    "ThreadDispatcher",
]
