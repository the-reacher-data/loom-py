"""ETL lineage records and lifecycle enums."""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Final

from loom.etl.schema import ColumnSchema, DatetimeType, LoomDtype


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
    """Field names for lineage records."""

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

    def to_row(self) -> dict[str, Any]:
        """Convert record into a storage row mapping."""
        return _record_to_row(self)


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

    def to_row(self) -> dict[str, Any]:
        """Convert record into a storage row mapping."""
        return _record_to_row(self)


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

    def to_row(self) -> dict[str, Any]:
        """Convert record into a storage row mapping."""
        return _record_to_row(self)


LineageRecord = PipelineRunRecord | ProcessRunRecord | StepRunRecord


def _record_to_row(record: Any) -> dict[str, Any]:
    row = dataclasses.asdict(record)
    row.pop("event", None)
    row["status"] = str(row["status"])
    return row


_PIPELINE_RECORD_SCHEMA: tuple[ColumnSchema, ...] = (
    ColumnSchema("run_id", LoomDtype.UTF8, nullable=False),
    ColumnSchema("correlation_id", LoomDtype.UTF8, nullable=True),
    ColumnSchema("attempt", LoomDtype.INT64, nullable=False),
    ColumnSchema("pipeline", LoomDtype.UTF8, nullable=False),
    ColumnSchema("started_at", DatetimeType("us", "UTC"), nullable=False),
    ColumnSchema("status", LoomDtype.UTF8, nullable=False),
    ColumnSchema("duration_ms", LoomDtype.INT64, nullable=False),
    ColumnSchema("error", LoomDtype.UTF8, nullable=True),
    ColumnSchema("error_type", LoomDtype.UTF8, nullable=True),
    ColumnSchema("error_message", LoomDtype.UTF8, nullable=True),
    ColumnSchema("failed_step_run_id", LoomDtype.UTF8, nullable=True),
    ColumnSchema("failed_step", LoomDtype.UTF8, nullable=True),
)

_PROCESS_RECORD_SCHEMA: tuple[ColumnSchema, ...] = (
    ColumnSchema("run_id", LoomDtype.UTF8, nullable=False),
    ColumnSchema("correlation_id", LoomDtype.UTF8, nullable=True),
    ColumnSchema("attempt", LoomDtype.INT64, nullable=False),
    ColumnSchema("process_run_id", LoomDtype.UTF8, nullable=False),
    ColumnSchema("process", LoomDtype.UTF8, nullable=False),
    ColumnSchema("started_at", DatetimeType("us", "UTC"), nullable=False),
    ColumnSchema("status", LoomDtype.UTF8, nullable=False),
    ColumnSchema("duration_ms", LoomDtype.INT64, nullable=False),
    ColumnSchema("error", LoomDtype.UTF8, nullable=True),
    ColumnSchema("error_type", LoomDtype.UTF8, nullable=True),
    ColumnSchema("error_message", LoomDtype.UTF8, nullable=True),
    ColumnSchema("failed_step_run_id", LoomDtype.UTF8, nullable=True),
    ColumnSchema("failed_step", LoomDtype.UTF8, nullable=True),
)

_STEP_RECORD_SCHEMA: tuple[ColumnSchema, ...] = (
    ColumnSchema("run_id", LoomDtype.UTF8, nullable=False),
    ColumnSchema("correlation_id", LoomDtype.UTF8, nullable=True),
    ColumnSchema("attempt", LoomDtype.INT64, nullable=False),
    ColumnSchema("step_run_id", LoomDtype.UTF8, nullable=False),
    ColumnSchema("step", LoomDtype.UTF8, nullable=False),
    ColumnSchema("started_at", DatetimeType("us", "UTC"), nullable=False),
    ColumnSchema("status", LoomDtype.UTF8, nullable=False),
    ColumnSchema("duration_ms", LoomDtype.INT64, nullable=False),
    ColumnSchema("error", LoomDtype.UTF8, nullable=True),
    ColumnSchema("process_run_id", LoomDtype.UTF8, nullable=True),
    ColumnSchema("error_type", LoomDtype.UTF8, nullable=True),
    ColumnSchema("error_message", LoomDtype.UTF8, nullable=True),
)

_RECORD_SCHEMA_MAP: Final[dict[type[LineageRecord], tuple[ColumnSchema, ...]]] = {
    PipelineRunRecord: _PIPELINE_RECORD_SCHEMA,
    ProcessRunRecord: _PROCESS_RECORD_SCHEMA,
    StepRunRecord: _STEP_RECORD_SCHEMA,
}


def get_lineage_schema(record_type: type[LineageRecord]) -> tuple[ColumnSchema, ...]:
    """Return the canonical schema tuple for *record_type*."""
    return _RECORD_SCHEMA_MAP[record_type]


_TABLE_MAP: dict[type[LineageRecord], str] = {
    PipelineRunRecord: RecordField.PIPELINE_RUNS,
    ProcessRunRecord: RecordField.PROCESS_RUNS,
    StepRunRecord: RecordField.STEP_RUNS,
}


def get_lineage_table_name(record_type: type[LineageRecord]) -> str:
    """Get table name for a given lineage record type."""
    return _TABLE_MAP[record_type]


__all__ = [
    "EventName",
    "LineageRecord",
    "PipelineRunRecord",
    "ProcessRunRecord",
    "RecordField",
    "RunContext",
    "RunStatus",
    "StepRunRecord",
    "get_lineage_schema",
    "get_lineage_table_name",
]
