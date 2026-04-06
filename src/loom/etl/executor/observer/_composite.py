"""CompositeObserver — fan-out with per-observer error isolation.

Internal module — import from :mod:`loom.etl.executor.observer`.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import structlog

from loom.etl.executor.observer._events import RunContext, RunStatus
from loom.etl.executor.observer._protocol import ETLRunObserver

_log: Any = structlog.get_logger("loom.etl.observer")


class CompositeObserver:
    """Fan-out observer that isolates failures across multiple observers.

    Calls each observer in order.  If one raises, the exception is logged
    at ``error`` level and the remaining observers are still called — an
    observability failure never interrupts pipeline execution.

    :class:`~loom.etl.ETLRunner` wraps all observers in a
    ``CompositeObserver`` automatically.

    Args:
        observers: Sequence of :class:`~loom.etl.executor.ETLRunObserver`
                   implementations to fan out to.

    Example::

        from loom.etl.executor import NoopRunObserver, StructlogRunObserver
        from loom.etl.executor.observer import CompositeObserver

        composite = CompositeObserver([
            StructlogRunObserver(),
            NoopRunObserver(),
        ])
    """

    def __init__(self, observers: Sequence[ETLRunObserver]) -> None:
        self._observers = tuple(observers)

    def on_pipeline_start(self, plan: Any, _params: Any, ctx: RunContext) -> None:
        for obs in self._observers:
            _safe(obs.on_pipeline_start, plan, _params, ctx)

    def on_pipeline_end(self, ctx: RunContext, status: RunStatus, duration_ms: int) -> None:
        for obs in self._observers:
            _safe(obs.on_pipeline_end, ctx, status, duration_ms)

    def on_process_start(self, plan: Any, ctx: RunContext, process_run_id: str) -> None:
        for obs in self._observers:
            _safe(obs.on_process_start, plan, ctx, process_run_id)

    def on_process_end(self, process_run_id: str, status: RunStatus, duration_ms: int) -> None:
        for obs in self._observers:
            _safe(obs.on_process_end, process_run_id, status, duration_ms)

    def on_step_start(self, plan: Any, ctx: RunContext, step_run_id: str) -> None:
        for obs in self._observers:
            _safe(obs.on_step_start, plan, ctx, step_run_id)

    def on_step_end(self, step_run_id: str, status: RunStatus, duration_ms: int) -> None:
        for obs in self._observers:
            _safe(obs.on_step_end, step_run_id, status, duration_ms)

    def on_step_error(self, step_run_id: str, exc: Exception) -> None:
        for obs in self._observers:
            _safe(obs.on_step_error, step_run_id, exc)


def _safe(fn: Any, *args: Any) -> None:
    try:
        fn(*args)
    except Exception as exc:  # noqa: BLE001 - observer callbacks are user code; isolate failures.
        _log.error(
            "observer_error",
            observer=type(fn.__self__).__name__ if hasattr(fn, "__self__") else repr(fn),
            error=repr(exc),
        )
