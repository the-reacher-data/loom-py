"""Observer that persists completed lifecycle records into an execution record store."""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from loom.etl.compiler._plan import PipelinePlan, ProcessPlan, StepPlan
from loom.etl.observability.records import (
    EventName,
    ExecutionRecord,
    PipelineRunRecord,
    ProcessRunRecord,
    RunContext,
    RunStatus,
    StepRunRecord,
)
from loom.etl.observability.sinks._protocol import ExecutionRecordStore


class ExecutionRecordsObserver:
    """Convert lifecycle callbacks into persisted execution records.

    Args:
        store: Persistence backend implementing :class:`ExecutionRecordStore`.
    """

    def __init__(self, store: ExecutionRecordStore) -> None:
        self._store = store
        self._write_lock = threading.Lock()
        self._pipeline_ctx: dict[str, _EntityContext] = {}
        self._process_ctx: dict[str, _EntityContext] = {}
        self._step_ctx: dict[str, _EntityContext] = {}
        self._step_errors: dict[str, _ErrorDetails] = {}
        self._process_failures: dict[str, _FailureContext] = {}
        self._pipeline_failures: dict[str, _FailureContext] = {}

    def on_pipeline_start(self, plan: PipelinePlan, _params: Any, ctx: RunContext) -> None:
        with self._write_lock:
            self._pipeline_ctx[ctx.run_id] = _EntityContext(
                run_ctx=ctx,
                name=plan.pipeline_type.__name__,
                started_at=_now(),
            )

    def on_pipeline_end(self, ctx: RunContext, status: RunStatus, duration_ms: int) -> None:
        record = self._create_record(
            run_id=ctx.run_id,
            context_dict=self._pipeline_ctx,
            failure_dict=self._pipeline_failures,
            status=status,
            duration_ms=duration_ms,
            record_factory=lambda ctx, name, failure: PipelineRunRecord(
                event=EventName.PIPELINE_END,
                run_id=ctx.run_ctx.run_id,
                correlation_id=ctx.run_ctx.correlation_id,
                attempt=ctx.run_ctx.attempt,
                pipeline=name,
                started_at=ctx.started_at,
                status=status,
                duration_ms=duration_ms,
                error=failure.error if failure else None,
                error_type=failure.error_type if failure else None,
                error_message=failure.error_message if failure else None,
                failed_step_run_id=failure.step_run_id if failure else None,
                failed_step=failure.step if failure else None,
            ),
        )
        if record:
            self._store.write_record(record)

    def on_process_start(self, plan: ProcessPlan, ctx: RunContext, process_run_id: str) -> None:
        with self._write_lock:
            self._process_ctx[process_run_id] = _EntityContext(
                run_ctx=ctx,
                name=plan.process_type.__name__,
                started_at=_now(),
            )

    def on_process_end(self, process_run_id: str, status: RunStatus, duration_ms: int) -> None:
        record = self._create_record(
            run_id=process_run_id,
            context_dict=self._process_ctx,
            failure_dict=self._process_failures,
            status=status,
            duration_ms=duration_ms,
            record_factory=lambda ctx, name, failure: ProcessRunRecord(
                event=EventName.PROCESS_END,
                run_id=ctx.run_ctx.run_id,
                correlation_id=ctx.run_ctx.correlation_id,
                attempt=ctx.run_ctx.attempt,
                process_run_id=process_run_id,
                process=name,
                started_at=ctx.started_at,
                status=status,
                duration_ms=duration_ms,
                error=failure.error if failure else None,
                error_type=failure.error_type if failure else None,
                error_message=failure.error_message if failure else None,
                failed_step_run_id=failure.step_run_id if failure else None,
                failed_step=failure.step if failure else None,
            ),
        )
        if record:
            self._store.write_record(record)

    def on_step_start(self, plan: StepPlan, ctx: RunContext, step_run_id: str) -> None:
        with self._write_lock:
            self._step_ctx[step_run_id] = _EntityContext(
                run_ctx=ctx,
                name=plan.step_type.__name__,
                started_at=_now(),
            )

    def on_step_end(self, step_run_id: str, status: RunStatus, duration_ms: int) -> None:
        failure: _FailureContext | None = None

        with self._write_lock:
            ctx = self._step_ctx.pop(step_run_id, None)
            if ctx is None:
                return
            error = self._step_errors.pop(step_run_id, None)
            if status is RunStatus.FAILED and error is not None:
                failure = _FailureContext(
                    step_run_id=step_run_id,
                    step=ctx.name,
                    error=error.error,
                    error_type=error.error_type,
                    error_message=error.error_message,
                )
                if ctx.run_ctx.process_run_id is not None:
                    self._process_failures[ctx.run_ctx.process_run_id] = failure
                self._pipeline_failures[ctx.run_ctx.run_id] = failure

        record = StepRunRecord(
            event=EventName.STEP_END,
            run_id=ctx.run_ctx.run_id,
            correlation_id=ctx.run_ctx.correlation_id,
            attempt=ctx.run_ctx.attempt,
            step_run_id=step_run_id,
            step=ctx.name,
            started_at=ctx.started_at,
            status=status,
            duration_ms=duration_ms,
            error=error.error if error else None,
            process_run_id=ctx.run_ctx.process_run_id,
            error_type=error.error_type if error else None,
            error_message=error.error_message if error else None,
        )
        self._store.write_record(record)

    def on_step_error(self, step_run_id: str, exc: Exception) -> None:
        with self._write_lock:
            self._step_errors[step_run_id] = _error_details(exc)

    def _create_record(
        self,
        run_id: str,
        context_dict: dict[str, _EntityContext],
        failure_dict: dict[str, _FailureContext],
        status: RunStatus,
        duration_ms: int,
        record_factory: Callable[[_EntityContext, str, _FailureContext | None], ExecutionRecord],
    ) -> ExecutionRecord | None:
        """Generic record creation helper for pipeline and process end events.

        Args:
            run_id: Unique identifier for the entity run.
            context_dict: Storage dict containing start context.
            failure_dict: Storage dict containing failure context.
            status: Terminal status of the run.
            duration_ms: Wall-clock duration in milliseconds.
            record_factory: Factory function to create the specific record type.

        Returns:
            The created record or None if context not found.
        """
        with self._write_lock:
            ctx = context_dict.pop(run_id, None)
            failure = failure_dict.pop(run_id, None)

        if ctx is None:
            return None

        return record_factory(ctx, ctx.name, failure)


def _now() -> datetime:
    return datetime.now(tz=UTC)


@dataclass(frozen=True)
class _EntityContext:
    """Captured context at entity start."""

    run_ctx: RunContext
    name: str
    started_at: datetime


@dataclass(frozen=True)
class _ErrorDetails:
    error: str
    error_type: str
    error_message: str


@dataclass(frozen=True)
class _FailureContext:
    step_run_id: str
    step: str
    error: str
    error_type: str
    error_message: str


def _error_details(exc: Exception) -> _ErrorDetails:
    return _ErrorDetails(
        error=repr(exc),
        error_type=type(exc).__name__,
        error_message=str(exc),
    )


__all__ = ["ExecutionRecordsObserver"]
