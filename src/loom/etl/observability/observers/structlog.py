"""Structured log observer for ETL lifecycle events."""

from __future__ import annotations

from typing import Any

import structlog

from loom.etl.io.target._file import FileSpec
from loom.etl.io.target._table import (
    AppendSpec,
    ReplacePartitionsSpec,
    ReplaceSpec,
    ReplaceWhereSpec,
    UpsertSpec,
)
from loom.etl.io.target._temp import TempFanInSpec, TempSpec
from loom.etl.observability.records import EventName, RunContext, RunStatus

_log: Any = structlog.get_logger("loom.etl")


class StructlogRunObserver:
    """Observer that emits structured lifecycle events through structlog.

    Args:
        slow_step_threshold_ms: Optional threshold to emit ``slow_step`` warnings.
    """

    def __init__(self, slow_step_threshold_ms: int | None = None) -> None:
        self._slow_ms = slow_step_threshold_ms

    def on_pipeline_start(self, plan: Any, _params: Any, ctx: RunContext) -> None:
        _log.info(
            EventName.PIPELINE_START,
            pipeline=plan.pipeline_type.__name__,
            run_id=ctx.run_id,
            correlation_id=ctx.correlation_id,
            attempt=ctx.attempt,
        )

    def on_pipeline_end(self, ctx: RunContext, status: RunStatus, duration_ms: int) -> None:
        _log.info(
            EventName.PIPELINE_END,
            run_id=ctx.run_id,
            correlation_id=ctx.correlation_id,
            attempt=ctx.attempt,
            status=status,
            duration_ms=duration_ms,
        )

    def on_process_start(self, plan: Any, ctx: RunContext, process_run_id: str) -> None:
        _log.info(
            EventName.PROCESS_START,
            process=plan.process_type.__name__,
            steps=len(plan.nodes),
            run_id=ctx.run_id,
            process_run_id=process_run_id,
        )

    def on_process_end(self, process_run_id: str, status: RunStatus, duration_ms: int) -> None:
        _log.info(
            EventName.PROCESS_END,
            process_run_id=process_run_id,
            status=status,
            duration_ms=duration_ms,
        )

    def on_step_start(self, plan: Any, ctx: RunContext, step_run_id: str) -> None:
        sources = [_source_label(binding.spec) for binding in plan.source_bindings]
        _log.info(
            EventName.STEP_START,
            step=plan.step_type.__name__,
            sources=sources,
            target=_target_label(plan.target_binding.spec),
            write_mode=_write_mode_label(plan.target_binding.spec),
            run_id=ctx.run_id,
            step_run_id=step_run_id,
        )

    def on_step_end(self, step_run_id: str, status: RunStatus, duration_ms: int) -> None:
        _log.info(
            EventName.STEP_END,
            step_run_id=step_run_id,
            status=status,
            duration_ms=duration_ms,
        )
        if self._slow_ms is not None and duration_ms > self._slow_ms:
            _log.warning(
                "slow_step",
                step_run_id=step_run_id,
                duration_ms=duration_ms,
                threshold_ms=self._slow_ms,
            )

    def on_step_error(self, step_run_id: str, exc: Exception) -> None:
        _log.error(
            EventName.STEP_ERROR,
            step_run_id=step_run_id,
            error=repr(exc),
        )


def _source_label(spec: Any) -> str:
    """Return a readable label for a source spec."""
    if spec.table_ref is not None:
        return str(spec.table_ref.ref)
    if getattr(spec, "temp_name", None) is not None:
        return f"temp:{spec.temp_name}"
    return str(spec.path) if spec.path else "?"


def _target_label(spec: Any) -> str:
    """Return a readable label for a target spec."""
    if spec.table_ref is not None:
        return str(spec.table_ref.ref)
    if getattr(spec, "temp_name", None) is not None:
        return f"temp:{spec.temp_name}"
    return str(spec.path) if spec.path else "?"


def _write_mode_label(spec: Any) -> str:
    """Return the write mode label for the given target spec."""
    match spec:
        case AppendSpec():
            return "append"
        case ReplaceSpec():
            return "replace"
        case ReplacePartitionsSpec():
            return "replace_partitions"
        case ReplaceWhereSpec():
            return "replace_where"
        case UpsertSpec():
            return "upsert"
        case TempSpec() | TempFanInSpec():
            return "temp"
        case FileSpec():
            return "file"
        case _:
            return "unknown"


__all__ = ["StructlogRunObserver", "_source_label", "_target_label", "_write_mode_label"]
