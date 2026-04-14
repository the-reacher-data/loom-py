"""Observer protocol for ETL lifecycle hooks."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from loom.etl.compiler._plan import PipelinePlan, ProcessPlan, StepPlan
from loom.etl.observability.records import RunContext, RunStatus


@runtime_checkable
class ETLRunObserver(Protocol):
    """Protocol for ETL lifecycle observability callbacks."""

    def on_pipeline_start(self, plan: PipelinePlan, _params: Any, ctx: RunContext) -> None:
        """Called before the first process of the pipeline executes."""

    def on_pipeline_end(self, ctx: RunContext, status: RunStatus, duration_ms: int) -> None:
        """Called after pipeline completion or first unhandled error."""

    def on_process_start(self, plan: ProcessPlan, ctx: RunContext, process_run_id: str) -> None:
        """Called before the first step of a process executes."""

    def on_process_end(self, process_run_id: str, status: RunStatus, duration_ms: int) -> None:
        """Called after process completion or first unhandled error."""

    def on_step_start(self, plan: StepPlan, ctx: RunContext, step_run_id: str) -> None:
        """Called before sources are read."""

    def on_step_end(self, step_run_id: str, status: RunStatus, duration_ms: int) -> None:
        """Called after write completes or on failure."""

    def on_step_error(self, step_run_id: str, exc: Exception) -> None:
        """Called before ``on_step_end`` when a step fails."""


__all__ = ["ETLRunObserver"]
