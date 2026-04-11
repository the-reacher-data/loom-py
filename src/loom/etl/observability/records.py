"""Execution observability records and lifecycle enums."""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any


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


class RecordField(StrEnum):
    """Field names for execution records."""

    PIPELINE_RUNS = "pipeline_runs"
    PROCESS_RUNS = "process_runs"
    STEP_RUNS = "step_runs"
    EVENT = "event"
    RUN_ID = "run_id"
    CORRELATION_ID = "correlation_id"
    ATTEMPT = "attempt"
    STARTED_AT = "started_at"
    STATUS = "status"
    DURATION_MS = "duration_ms"
    ERROR = "error"
    ERROR_TYPE = "error_type"
    ERROR_MESSAGE = "error_message"
    FAILED_STEP_RUN_ID = "failed_step_run_id"
    FAILED_STEP = "failed_step"
    PIPELINE = "pipeline"
    PROCESS_RUN_ID = "process_run_id"
    PROCESS = "process"
    STEP_RUN_ID = "step_run_id"
    STEP = "step"


@dataclass(frozen=True)
class RunContext:
    """Execution context shared across all events of a pipeline attempt."""

    run_id: str
    correlation_id: str | None = None
    attempt: int = 1
    last_attempt: bool = True
    process_run_id: str | None = None


@dataclass(frozen=True)
class PipelineRunRecord:
    """Snapshot of a completed pipeline run."""

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

_TABLE_MAP: dict[type[ExecutionRecord], str] = {
    PipelineRunRecord: RecordField.PIPELINE_RUNS,
    ProcessRunRecord: RecordField.PROCESS_RUNS,
    StepRunRecord: RecordField.STEP_RUNS,
}


def get_record_table_name(record_type: type[ExecutionRecord]) -> str:
    """Get table name for a given record type."""
    return _TABLE_MAP[record_type]


def execution_record_to_row(record: ExecutionRecord) -> dict[str, Any]:
    """Convert execution record dataclass into a row mapping for sinks."""
    row = dataclasses.asdict(record)
    row.pop("event", None)
    row["status"] = str(row["status"])
    return row


__all__ = [
    "EventName",
    "ExecutionRecord",
    "execution_record_to_row",
    "get_record_table_name",
    "PipelineRunRecord",
    "ProcessRunRecord",
    "RecordField",
    "RunContext",
    "RunStatus",
    "StepRunRecord",
]
