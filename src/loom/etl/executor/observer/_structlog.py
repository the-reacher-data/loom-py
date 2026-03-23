"""StructlogRunObserver — structured events via structlog.

Each lifecycle event is emitted as a structlog log entry keyed by
:class:`~loom.etl.executor.EventName`.  All fields are passed as
keyword arguments — never interpolated into the message string — so
processors (JSON, Datadog, …) receive fully structured data.

Internal module — import from :mod:`loom.etl.executor.observer`.
"""

from __future__ import annotations

from typing import Any

import structlog

from loom.etl.executor.observer._events import EventName, RunStatus

_log: Any = structlog.get_logger("loom.etl")


class StructlogRunObserver:
    """Observer that emits fully structured events via structlog.

    Every field is a keyword argument — the event key is an
    :class:`~loom.etl.executor.EventName` constant, never a raw string.

    Compatible with any structlog processor chain (JSON, console, Datadog…).

    Example::

        import structlog
        structlog.configure(processors=[structlog.processors.JSONRenderer()])
        executor = ETLExecutor(reader, writer, observer=StructlogRunObserver())
    """

    def on_pipeline_start(self, plan: Any, _params: Any, run_id: str) -> None:
        _log.info(
            EventName.PIPELINE_START,
            pipeline=plan.pipeline_type.__name__,
            run_id=run_id,
        )

    def on_pipeline_end(self, run_id: str, status: RunStatus, duration_ms: int) -> None:
        _log.info(
            EventName.PIPELINE_END,
            run_id=run_id,
            status=status,
            duration_ms=duration_ms,
        )

    def on_process_start(self, plan: Any, run_id: str, process_run_id: str) -> None:
        _log.info(
            EventName.PROCESS_START,
            process=plan.process_type.__name__,
            run_id=run_id,
            process_run_id=process_run_id,
        )

    def on_process_end(self, process_run_id: str, status: RunStatus, duration_ms: int) -> None:
        _log.info(
            EventName.PROCESS_END,
            process_run_id=process_run_id,
            status=status,
            duration_ms=duration_ms,
        )

    def on_step_start(self, plan: Any, run_id: str, step_run_id: str) -> None:
        _log.info(
            EventName.STEP_START,
            step=plan.step_type.__name__,
            run_id=run_id,
            step_run_id=step_run_id,
        )

    def on_step_end(
        self,
        step_run_id: str,
        status: RunStatus,
        duration_ms: int,
    ) -> None:
        _log.info(
            EventName.STEP_END,
            step_run_id=step_run_id,
            status=status,
            duration_ms=duration_ms,
        )

    def on_step_error(self, step_run_id: str, exc: Exception) -> None:
        _log.error(
            EventName.STEP_ERROR,
            step_run_id=step_run_id,
            error=repr(exc),
        )
