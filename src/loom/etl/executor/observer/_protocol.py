"""ETLRunObserver — lifecycle observer protocol.

Internal module — import from :mod:`loom.etl.executor.observer`.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from loom.etl.compiler._plan import PipelinePlan, ProcessPlan, StepPlan
from loom.etl.executor.observer._events import RunContext, RunStatus


@runtime_checkable
class ETLRunObserver(Protocol):
    """Protocol for ETL lifecycle observability.

    Implementations receive structured events at each phase of execution.
    The :class:`~loom.etl.executor.observer._events.RunContext` passed to
    ``on_pipeline_start`` carries ``run_id``, ``correlation_id``, and
    ``attempt`` — buffer it to correlate start/end events and to include
    retry context in persisted records.

    Implement this protocol to route events to any backend:
    structured logs, Delta tables, OpenTelemetry collectors, etc.

    Example::

        class MyObserver:
            def on_pipeline_start(
                self, plan: PipelinePlan, params: Any, ctx: RunContext
            ) -> None:
                print(f"pipeline={plan.pipeline_type.__name__} attempt={ctx.attempt}")

            def on_step_end(
                self, step_run_id: str, status: RunStatus, duration_ms: int
            ) -> None:
                print(f"step={step_run_id} {status} {duration_ms}ms")
    """

    def on_pipeline_start(self, plan: PipelinePlan, params: Any, ctx: RunContext) -> None:
        """Called before the first process of the pipeline executes."""

    def on_pipeline_end(self, ctx: RunContext, status: RunStatus, duration_ms: int) -> None:
        """Called after all processes complete or on first unhandled error."""

    def on_process_start(self, plan: ProcessPlan, ctx: RunContext, process_run_id: str) -> None:
        """Called before the first step of a process executes."""

    def on_process_end(self, process_run_id: str, status: RunStatus, duration_ms: int) -> None:
        """Called after all steps of the process complete or on error."""

    def on_step_start(self, plan: StepPlan, ctx: RunContext, step_run_id: str) -> None:
        """Called before sources are read."""

    def on_step_end(self, step_run_id: str, status: RunStatus, duration_ms: int) -> None:
        """Called after the target write completes or on error.

        Args:
            step_run_id:  Unique ID for this step execution.
            status:       ``SUCCESS`` or ``FAILED``.
            duration_ms:  Wall-clock time for the full step (read+exec+write).
        """

    def on_step_error(self, step_run_id: str, exc: Exception) -> None:
        """Called with the unhandled exception before ``on_step_end(FAILED)``."""
