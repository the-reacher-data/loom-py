"""Observer package — ETL lifecycle observability.

Contains the observer protocol, event types, the run sink protocol,
and all built-in implementations.

Import from here, never from the internal submodules directly.

Contracts:

* :class:`ETLRunObserver`   — lifecycle observer protocol.
* :class:`RunSink`          — persistence sink protocol for run records.

Implementations:

* :class:`StructlogRunObserver` — fully structured events via structlog.
* :class:`RunSinkObserver`      — routes lifecycle events to a :class:`RunSink`.
"""

from loom.etl.executor.observer._events import (
    EventName,
    PipelineRunRecord,
    ProcessRunRecord,
    RunRecord,
    RunStatus,
    StepRunRecord,
)
from loom.etl.executor.observer._protocol import ETLRunObserver
from loom.etl.executor.observer._sink import RunSink
from loom.etl.executor.observer._sink_observer import RunSinkObserver
from loom.etl.executor.observer._structlog import StructlogRunObserver

__all__ = [
    # contracts
    "ETLRunObserver",
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
]
