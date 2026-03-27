"""Observer package — ETL lifecycle observability."""

from loom.etl.executor.observer._composite import CompositeObserver
from loom.etl.executor.observer._events import (
    EventName,
    PipelineRunRecord,
    ProcessRunRecord,
    RunContext,
    RunRecord,
    RunStatus,
    StepRunRecord,
)
from loom.etl.executor.observer._noop import NoopRunObserver
from loom.etl.executor.observer._protocol import ETLRunObserver
from loom.etl.executor.observer._sink_observer import RunSinkObserver
from loom.etl.executor.observer._structlog import StructlogRunObserver
from loom.etl.executor.observer.sinks.protocol import RunSink

__all__ = [
    # contracts
    "ETLRunObserver",
    "RunSink",
    # run context
    "RunContext",
    # events and records
    "EventName",
    "RunStatus",
    "RunRecord",
    "PipelineRunRecord",
    "ProcessRunRecord",
    "StepRunRecord",
    # implementations
    "CompositeObserver",
    "NoopRunObserver",
    "StructlogRunObserver",
    "RunSinkObserver",
]
