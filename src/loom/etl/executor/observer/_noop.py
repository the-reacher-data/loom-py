"""NoopRunObserver — lifecycle observer with no side effects.

Internal module — import from :mod:`loom.etl.executor.observer`.
"""

from __future__ import annotations

from typing import Any

from loom.etl.compiler._plan import PipelinePlan, ProcessPlan, StepPlan
from loom.etl.executor.observer._events import RunContext, RunStatus


class NoopRunObserver:
    """Observer implementation that intentionally does nothing.

    Useful when code expects an :class:`~loom.etl.executor.ETLRunObserver`
    instance but you explicitly do not want logs or persisted run records.
    """

    def on_pipeline_start(self, _plan: PipelinePlan, _params: Any, _ctx: RunContext) -> None:
        """No-op hook."""

    def on_pipeline_end(self, _ctx: RunContext, _status: RunStatus, _duration_ms: int) -> None:
        """No-op hook."""

    def on_process_start(self, _plan: ProcessPlan, _ctx: RunContext, _process_run_id: str) -> None:
        """No-op hook."""

    def on_process_end(self, _process_run_id: str, _status: RunStatus, _duration_ms: int) -> None:
        """No-op hook."""

    def on_step_start(self, _plan: StepPlan, _ctx: RunContext, _step_run_id: str) -> None:
        """No-op hook."""

    def on_step_end(self, _step_run_id: str, _status: RunStatus, _duration_ms: int) -> None:
        """No-op hook."""

    def on_step_error(self, _step_run_id: str, _exc: Exception) -> None:
        """No-op hook."""
