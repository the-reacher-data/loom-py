"""Structured log observer for ETL lifecycle events."""

from __future__ import annotations

from typing import Any

import structlog

from loom.etl.observability.observers._labels import (
    source_label,
    target_label,
    write_mode_label,
)
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
        sources = [source_label(binding.spec) for binding in plan.source_bindings]
        _log.info(
            EventName.STEP_START,
            step=plan.step_type.__name__,
            sources=sources,
            target=target_label(plan.target_binding.spec),
            write_mode=write_mode_label(plan.target_binding.spec),
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


__all__ = ["StructlogRunObserver", "source_label", "target_label", "write_mode_label"]
