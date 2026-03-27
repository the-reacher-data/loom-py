"""ETL executor public API.

The normal entry point is :class:`~loom.etl.ETLRunner` — it wires I/O,
compilation, and execution from a single YAML config.

This module exposes the observer and dispatcher abstractions so users can
compose observability pipelines without touching I/O internals:

* :class:`ETLRunObserver`    — lifecycle hook protocol
* :class:`NoopRunObserver`   — no-op implementation (zero side effects)
* :class:`StructlogRunObserver` — structured-log implementation
* :class:`RunSinkObserver`   — persists run records to a Delta sink
* :class:`ParallelDispatcher` / :class:`ThreadDispatcher` — parallelism

``ETLExecutor`` is intentionally **not** exported here.  It is an internal
engine used exclusively by :class:`~loom.etl.ETLRunner`.
"""

from loom.etl.executor._dispatcher import ParallelDispatcher, ThreadDispatcher
from loom.etl.executor._executor import ETLExecutor as ETLExecutor  # internal — not in __all__
from loom.etl.executor.observer import (
    CompositeObserver,
    ETLRunObserver,
    EventName,
    NoopRunObserver,
    PipelineRunRecord,
    ProcessRunRecord,
    RunContext,
    RunRecord,
    RunSink,
    RunSinkObserver,
    RunStatus,
    StepRunRecord,
    StructlogRunObserver,
)

__all__ = [
    # observer protocol
    "ETLRunObserver",
    # run sink protocol
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
    # dispatcher protocol + implementations
    "ParallelDispatcher",
    "ThreadDispatcher",
]
