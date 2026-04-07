"""Execution observability records and lifecycle enums."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class RunStatus(StrEnum):
    """Terminal status of a pipeline, process, or step run."""

    SUCCESS = "success"
    FAILED = "failed"


class EventName(StrEnum):
    """All lifecycle event names emitted by the ETL executor."""

    PIPELINE_START = "pipeline_start"
    PIPELINE_END = "pipeline_end"
    PROCESS_START = "process_start"
    PROCESS_END = "process_end"
    STEP_START = "step_start"
    STEP_END = "step_end"
    STEP_ERROR = "step_error"


@dataclass(frozen=True)
class RunContext:
    """Execution context shared across all events of a pipeline attempt.

    Args:
        run_id: UUID generated per attempt.
        correlation_id: Optional logical job id grouping retries.
        attempt: 1-based attempt counter.
        last_attempt: ``True`` when no more retries are expected.
        process_run_id: Optional current process id when executing inside a process.
    """

    run_id: str
    correlation_id: str | None = None
    attempt: int = 1
    last_attempt: bool = True
    process_run_id: str | None = None


@dataclass(frozen=True)
class PipelineRunRecord:
    """Snapshot of a completed pipeline run.

    Args:
        event: Lifecycle event that produced this record.
        run_id: UUID unique to this attempt.
        correlation_id: Retry-group identifier.
        attempt: 1-based retry counter.
        pipeline: Pipeline class name.
        started_at: UTC timestamp when pipeline started.
        status: Terminal status.
        duration_ms: Wall-clock duration in milliseconds.
        error: ``repr(exc)`` on failure, ``None`` on success.
        error_type: Exception class name when failed.
        error_message: Exception message when failed.
        failed_step_run_id: Step run id that caused the failure.
        failed_step: Step class name that caused the failure.
    """

    event: EventName
    run_id: str
    correlation_id: str | None
    attempt: int
    pipeline: str
    started_at: datetime
    status: RunStatus
    duration_ms: int
    error: str | None
    error_type: str | None = None
    error_message: str | None = None
    failed_step_run_id: str | None = None
    failed_step: str | None = None


@dataclass(frozen=True)
class ProcessRunRecord:
    """Snapshot of a completed process run."""

    event: EventName
    run_id: str
    correlation_id: str | None
    attempt: int
    process_run_id: str
    process: str
    started_at: datetime
    status: RunStatus
    duration_ms: int
    error: str | None
    error_type: str | None = None
    error_message: str | None = None
    failed_step_run_id: str | None = None
    failed_step: str | None = None


@dataclass(frozen=True)
class StepRunRecord:
    """Snapshot of a completed step run."""

    event: EventName
    run_id: str
    correlation_id: str | None
    attempt: int
    step_run_id: str
    step: str
    started_at: datetime
    status: RunStatus
    duration_ms: int
    error: str | None
    process_run_id: str | None = None
    error_type: str | None = None
    error_message: str | None = None


ExecutionRecord = PipelineRunRecord | ProcessRunRecord | StepRunRecord


__all__ = [
    "EventName",
    "ExecutionRecord",
    "PipelineRunRecord",
    "ProcessRunRecord",
    "RunContext",
    "RunStatus",
    "StepRunRecord",
]
