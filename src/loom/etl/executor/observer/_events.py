"""ETL run event names, statuses, and record types.

All observer implementations and the run sink share these definitions.
``EventName`` is the single source of truth for lifecycle event keys —
used as structlog ``event=`` values, protocol method suffixes, and record
discriminators.

Internal module — not part of the public API.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class RunStatus(StrEnum):
    """Terminal status of a pipeline, process, or step run."""

    SUCCESS = "success"
    FAILED = "failed"


class EventName(StrEnum):
    """All lifecycle event names emitted by the executor.

    Use these constants instead of raw strings when filtering or asserting
    on observer events (e.g. in :class:`~loom.etl.testing.StubRunObserver`).
    """

    PIPELINE_START = "pipeline_start"
    PIPELINE_END = "pipeline_end"
    PROCESS_START = "process_start"
    PROCESS_END = "process_end"
    STEP_START = "step_start"
    STEP_END = "step_end"
    STEP_ERROR = "step_error"


@dataclass(frozen=True)
class PipelineRunRecord:
    """Snapshot of a completed pipeline run written to a :class:`RunSink`.

    Args:
        event:       Lifecycle event that produced this record.
        run_id:      UUID of the pipeline run.
        pipeline:    Pipeline class name.
        started_at:  UTC timestamp when the pipeline started.
        status:      Terminal status.
        duration_ms: Wall-clock time for the full pipeline.
        error:       ``repr(exc)`` on failure, ``None`` on success.
    """

    event: EventName
    run_id: str
    pipeline: str
    started_at: datetime
    status: RunStatus
    duration_ms: int
    error: str | None


@dataclass(frozen=True)
class ProcessRunRecord:
    """Snapshot of a completed process run written to a :class:`RunSink`.

    Args:
        event:           Lifecycle event that produced this record.
        run_id:          Parent pipeline run UUID.
        process_run_id:  UUID of this process run.
        process:         Process class name.
        started_at:      UTC timestamp when the process started.
        status:          Terminal status.
        duration_ms:     Wall-clock time for the full process.
        error:           ``repr(exc)`` on failure, ``None`` on success.
    """

    event: EventName
    run_id: str
    process_run_id: str
    process: str
    started_at: datetime
    status: RunStatus
    duration_ms: int
    error: str | None


@dataclass(frozen=True)
class StepRunRecord:
    """Snapshot of a completed step run written to a :class:`RunSink`.

    Args:
        event:        Lifecycle event that produced this record.
        run_id:       Parent pipeline run UUID.
        step_run_id:  UUID of this step run.
        step:         Step class name.
        started_at:   UTC timestamp when the step started.
        status:       Terminal status.
        duration_ms:  Wall-clock time for read + execute + write.
        error:        ``repr(exc)`` on failure, ``None`` on success.
    """

    event: EventName
    run_id: str
    step_run_id: str
    step: str
    started_at: datetime
    status: RunStatus
    duration_ms: int
    error: str | None


RunRecord = PipelineRunRecord | ProcessRunRecord | StepRunRecord
