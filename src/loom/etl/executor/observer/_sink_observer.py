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

import threading
from datetime import UTC, datetime
from typing import Any

from loom.etl.executor.observer._events import (
    EventName,
    PipelineRunRecord,
    ProcessRunRecord,
    RunContext,
    RunStatus,
    StepRunRecord,
)
from loom.etl.executor.observer._sink import RunSink


class RunSinkObserver:
    """Adapter that converts lifecycle events into run records and persists them.

    Buffers start-event context (name, parent IDs, run context) in memory and
    emits one complete record per unit of work on the corresponding ``on_*_end``
    call.  Step errors are captured via ``on_step_error`` and included in the
    subsequent ``on_step_end`` record.

    Thread-safe: parallel step groups call ``on_step_start`` / ``on_step_end``
    concurrently.  A lock serialises sink writes so Delta transactions do not
    interleave.

    Args:
        sink: Persistence backend implementing :class:`~loom.etl.executor.RunSink`.

    Example::

        from loom.etl import ETLRunner
        from loom.etl.executor import RunSinkObserver
        from loom.etl.executor.observer.sinks import DeltaRunSink

        sink   = DeltaRunSink("s3://my-lake/runs/")
        runner = ETLRunner.from_yaml("loom.yaml", observers=[RunSinkObserver(sink)])
    """

    def __init__(self, sink: RunSink) -> None:
        self._sink = sink
        self._write_lock = threading.Lock()
        # run_id → (ctx, pipeline name, started_at)
        self._pipeline_ctx: dict[str, tuple[RunContext, str, datetime]] = {}
        # process_run_id → (ctx, process name, started_at)
        self._process_ctx: dict[str, tuple[RunContext, str, datetime]] = {}
        # step_run_id → (ctx, step name, started_at)
        self._step_ctx: dict[str, tuple[RunContext, str, datetime]] = {}
        # step_run_id → repr(exc)
        self._step_errors: dict[str, str] = {}

    def on_pipeline_start(self, plan: Any, _params: Any, ctx: RunContext) -> None:
        self._pipeline_ctx[ctx.run_id] = (ctx, plan.pipeline_type.__name__, _now())

    def on_pipeline_end(self, ctx: RunContext, status: RunStatus, duration_ms: int) -> None:
        stored_ctx, pipeline, started_at = self._pipeline_ctx.pop(ctx.run_id)
        record = PipelineRunRecord(
            event=EventName.PIPELINE_END,
            run_id=stored_ctx.run_id,
            correlation_id=stored_ctx.correlation_id,
            attempt=stored_ctx.attempt,
            pipeline=pipeline,
            started_at=started_at,
            status=status,
            duration_ms=duration_ms,
            error=None,
        )
        with self._write_lock:
            self._sink.write(record)

    def on_process_start(self, plan: Any, ctx: RunContext, process_run_id: str) -> None:
        self._process_ctx[process_run_id] = (ctx, plan.process_type.__name__, _now())

    def on_process_end(self, process_run_id: str, status: RunStatus, duration_ms: int) -> None:
        stored_ctx, process, started_at = self._process_ctx.pop(process_run_id)
        record = ProcessRunRecord(
            event=EventName.PROCESS_END,
            run_id=stored_ctx.run_id,
            correlation_id=stored_ctx.correlation_id,
            attempt=stored_ctx.attempt,
            process_run_id=process_run_id,
            process=process,
            started_at=started_at,
            status=status,
            duration_ms=duration_ms,
            error=None,
        )
        with self._write_lock:
            self._sink.write(record)

    def on_step_start(self, plan: Any, ctx: RunContext, step_run_id: str) -> None:
        self._step_ctx[step_run_id] = (ctx, plan.step_type.__name__, _now())

    def on_step_end(self, step_run_id: str, status: RunStatus, duration_ms: int) -> None:
        stored_ctx, step, started_at = self._step_ctx.pop(step_run_id)
        error = self._step_errors.pop(step_run_id, None)
        record = StepRunRecord(
            event=EventName.STEP_END,
            run_id=stored_ctx.run_id,
            correlation_id=stored_ctx.correlation_id,
            attempt=stored_ctx.attempt,
            step_run_id=step_run_id,
            step=step,
            started_at=started_at,
            status=status,
            duration_ms=duration_ms,
            error=error,
        )
        with self._write_lock:
            self._sink.write(record)

    def on_step_error(self, step_run_id: str, exc: Exception) -> None:
        self._step_errors[step_run_id] = repr(exc)


def _now() -> datetime:
    return datetime.now(tz=UTC)
