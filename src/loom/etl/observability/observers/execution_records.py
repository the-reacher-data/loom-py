"""Observer that persists completed lifecycle records into an execution record store."""

from __future__ import annotations

import threading
from datetime import UTC, datetime
from typing import Any

from loom.etl.observability.records import (
    EventName,
    PipelineRunRecord,
    ProcessRunRecord,
    RunContext,
    RunStatus,
    StepRunRecord,
)
from loom.etl.observability.stores.protocol import ExecutionRecordStore


class ExecutionRecordsObserver:
    """Convert lifecycle callbacks into persisted execution records.

    Args:
        store: Persistence backend implementing :class:`ExecutionRecordStore`.
    """

    def __init__(self, store: ExecutionRecordStore) -> None:
        self._store = store
        self._write_lock = threading.Lock()
        self._pipeline_ctx: dict[str, tuple[RunContext, str, datetime]] = {}
        self._process_ctx: dict[str, tuple[RunContext, str, datetime]] = {}
        self._step_ctx: dict[str, tuple[RunContext, str, datetime]] = {}
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
            self._store.write_record(record)

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
            self._store.write_record(record)

    def on_step_start(self, plan: Any, ctx: RunContext, step_run_id: str) -> None:
        with self._write_lock:
            self._step_ctx[step_run_id] = (ctx, plan.step_type.__name__, _now())

    def on_step_end(self, step_run_id: str, status: RunStatus, duration_ms: int) -> None:
        with self._write_lock:
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
            self._store.write_record(record)

    def on_step_error(self, step_run_id: str, exc: Exception) -> None:
        with self._write_lock:
            self._step_errors[step_run_id] = repr(exc)


def _now() -> datetime:
    return datetime.now(tz=UTC)


__all__ = ["ExecutionRecordsObserver"]
