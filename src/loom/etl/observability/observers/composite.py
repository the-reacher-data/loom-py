"""Composite observer with per-observer error isolation."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import structlog

from loom.etl.observability.observers.protocol import ETLRunObserver
from loom.etl.observability.records import RunContext, RunStatus

_log: Any = structlog.get_logger("loom.etl.observer")


class CompositeObserver:
    """Fan out lifecycle events to multiple observers safely."""

    def __init__(self, observers: Sequence[ETLRunObserver]) -> None:
        self._observers = tuple(observers)

    def on_pipeline_start(self, plan: Any, _params: Any, ctx: RunContext) -> None:
        for observer in self._observers:
            _safe(observer.on_pipeline_start, plan, _params, ctx)

    def on_pipeline_end(self, ctx: RunContext, status: RunStatus, duration_ms: int) -> None:
        for observer in self._observers:
            _safe(observer.on_pipeline_end, ctx, status, duration_ms)

    def on_process_start(self, plan: Any, ctx: RunContext, process_run_id: str) -> None:
        for observer in self._observers:
            _safe(observer.on_process_start, plan, ctx, process_run_id)

    def on_process_end(self, process_run_id: str, status: RunStatus, duration_ms: int) -> None:
        for observer in self._observers:
            _safe(observer.on_process_end, process_run_id, status, duration_ms)

    def on_step_start(self, plan: Any, ctx: RunContext, step_run_id: str) -> None:
        for observer in self._observers:
            _safe(observer.on_step_start, plan, ctx, step_run_id)

    def on_step_end(self, step_run_id: str, status: RunStatus, duration_ms: int) -> None:
        for observer in self._observers:
            _safe(observer.on_step_end, step_run_id, status, duration_ms)

    def on_step_error(self, step_run_id: str, exc: Exception) -> None:
        for observer in self._observers:
            _safe(observer.on_step_error, step_run_id, exc)


def _safe(fn: Any, *args: Any) -> None:
    try:
        fn(*args)
    except Exception as exc:  # noqa: BLE001 - callbacks are user code; failures are isolated.
        _log.error(
            "observer_error",
            observer=type(fn.__self__).__name__ if hasattr(fn, "__self__") else repr(fn),
            error=repr(exc),
        )


__all__ = ["CompositeObserver"]
