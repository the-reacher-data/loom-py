"""RunSinkObserver — adapter from observer events to run sink records.

Bridges the :class:`~loom.etl.executor.observer.ETLRunObserver` protocol and
the :class:`~loom.etl.executor.observer.RunSink` persistence protocol.

Lifecycle events carry partial data (start events carry names, end events carry
status/duration).  This observer buffers context from ``on_*_start`` calls and
assembles complete :mod:`~loom.etl.executor.observer._events` records on
``on_*_end``.

Internal module — import from :mod:`loom.etl.executor.observer`.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from loom.etl.executor.observer._events import (
    EventName,
    PipelineRunRecord,
    ProcessRunRecord,
    RunStatus,
    StepRunRecord,
)
from loom.etl.executor.observer._sink import RunSink


class RunSinkObserver:
    """Adapter that converts lifecycle events into run records and persists them.

    Buffers start-event context (name, parent IDs) in memory and emits one
    complete record per unit of work on the corresponding ``on_*_end`` call.
    Step errors are captured via ``on_step_error`` and included in the
    subsequent ``on_step_end`` record.

    Args:
        sink: Persistence backend implementing :class:`~loom.etl.executor.RunSink`.

    Example::

        from pathlib import Path
        from loom.etl.backends.delta import DeltaRunSink
        from loom.etl.executor import ETLExecutor, RunSinkObserver

        sink     = DeltaRunSink(root=Path("/data/runs"))
        observer = RunSinkObserver(sink)
        executor = ETLExecutor(reader, writer, observers=[observer])
    """

    def __init__(self, sink: RunSink) -> None:
        self._sink = sink
        # run_id → (pipeline name, started_at)
        self._pipeline_ctx: dict[str, tuple[str, datetime]] = {}
        # process_run_id → (run_id, process name, started_at)
        self._process_ctx: dict[str, tuple[str, str, datetime]] = {}
        # step_run_id → (run_id, step name, started_at)
        self._step_ctx: dict[str, tuple[str, str, datetime]] = {}
        # step_run_id → repr(exc)  — populated by on_step_error
        self._step_errors: dict[str, str] = {}

    def on_pipeline_start(self, plan: Any, _params: Any, run_id: str) -> None:
        self._pipeline_ctx[run_id] = (plan.pipeline_type.__name__, _now())

    def on_pipeline_end(self, run_id: str, status: RunStatus, duration_ms: int) -> None:
        pipeline, started_at = self._pipeline_ctx.pop(run_id)
        self._sink.write(
            PipelineRunRecord(
                event=EventName.PIPELINE_END,
                run_id=run_id,
                pipeline=pipeline,
                started_at=started_at,
                status=status,
                duration_ms=duration_ms,
                error=None,
            )
        )

    def on_process_start(self, plan: Any, run_id: str, process_run_id: str) -> None:
        self._process_ctx[process_run_id] = (run_id, plan.process_type.__name__, _now())

    def on_process_end(self, process_run_id: str, status: RunStatus, duration_ms: int) -> None:
        run_id, process, started_at = self._process_ctx.pop(process_run_id)
        self._sink.write(
            ProcessRunRecord(
                event=EventName.PROCESS_END,
                run_id=run_id,
                process_run_id=process_run_id,
                process=process,
                started_at=started_at,
                status=status,
                duration_ms=duration_ms,
                error=None,
            )
        )

    def on_step_start(self, plan: Any, run_id: str, step_run_id: str) -> None:
        self._step_ctx[step_run_id] = (run_id, plan.step_type.__name__, _now())

    def on_step_end(
        self,
        step_run_id: str,
        status: RunStatus,
        duration_ms: int,
    ) -> None:
        run_id, step, started_at = self._step_ctx.pop(step_run_id)
        error = self._step_errors.pop(step_run_id, None)
        self._sink.write(
            StepRunRecord(
                event=EventName.STEP_END,
                run_id=run_id,
                step_run_id=step_run_id,
                step=step,
                started_at=started_at,
                status=status,
                duration_ms=duration_ms,
                error=error,
            )
        )

    def on_step_error(self, step_run_id: str, exc: Exception) -> None:
        self._step_errors[step_run_id] = repr(exc)


def _now() -> datetime:
    return datetime.now(tz=UTC)
