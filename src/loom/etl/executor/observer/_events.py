"""ETL run event names, statuses, context, and record types.

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
class RunContext:
    """Execution context shared across all events of a single pipeline run.

    Passed to ``on_pipeline_start`` and forwarded by observers to correlate
    all records (pipeline, process, step) from the same logical execution.

    Args:
        run_id:         UUID generated per attempt — unique per execution.
        correlation_id: Opaque ID supplied by the caller to group retry
                        attempts of the same logical job.  Typically the
                        orchestrator's job run ID (Databricks, Bytewarx, etc.).
                        ``None`` when not provided.
        attempt:        1-based attempt counter.  Pass the orchestrator's
                        retry number so retries are distinguishable in the
                        run tables.  Defaults to ``1``.
        last_attempt:   ``True`` when no further retry will be attempted.
                        Defaults to ``True`` — safe for non-orchestrated runs.
                        Set to ``False`` from your orchestrator when more
                        retries are configured, so the executor keeps
                        ``CORRELATION`` intermediates alive for the next
                        attempt.

    Example (Databricks)::

        runner.run(
            DailyOrdersPipeline,
            params,
            correlation_id=dbutils.jobs.taskValues.get("job_run_id"),
            attempt=dbutils.jobs.taskValues.get("attempt_number"),
        )
    """

    run_id: str
    correlation_id: str | None = None
    attempt: int = 1
    last_attempt: bool = True


@dataclass(frozen=True)
class PipelineRunRecord:
    """Snapshot of a completed pipeline run written to a :class:`RunSink`.

    Args:
        event:          Lifecycle event that produced this record.
        run_id:         UUID unique to this attempt.
        correlation_id: Groups retry attempts of the same logical job.
        attempt:        1-based retry counter.
        pipeline:       Pipeline class name.
        started_at:     UTC timestamp when the pipeline started.
        status:         Terminal status.
        duration_ms:    Wall-clock time for the full pipeline.
        error:          ``repr(exc)`` on failure, ``None`` on success.
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


@dataclass(frozen=True)
class ProcessRunRecord:
    """Snapshot of a completed process run written to a :class:`RunSink`.

    Args:
        event:          Lifecycle event that produced this record.
        run_id:         Parent pipeline run UUID.
        correlation_id: Groups retry attempts of the same logical job.
        attempt:        1-based retry counter.
        process_run_id: UUID of this process run.
        process:        Process class name.
        started_at:     UTC timestamp when the process started.
        status:         Terminal status.
        duration_ms:    Wall-clock time for the full process.
        error:          ``repr(exc)`` on failure, ``None`` on success.
    """

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


@dataclass(frozen=True)
class StepRunRecord:
    """Snapshot of a completed step run written to a :class:`RunSink`.

    Args:
        event:          Lifecycle event that produced this record.
        run_id:         Parent pipeline run UUID.
        correlation_id: Groups retry attempts of the same logical job.
        attempt:        1-based retry counter.
        step_run_id:    UUID of this step run.
        step:           Step class name.
        started_at:     UTC timestamp when the step started.
        status:         Terminal status.
        duration_ms:    Wall-clock time for read + execute + write.
        error:          ``repr(exc)`` on failure, ``None`` on success.
    """

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


RunRecord = PipelineRunRecord | ProcessRunRecord | StepRunRecord
