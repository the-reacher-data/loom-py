"""StructlogRunObserver — structured events via structlog.

Each lifecycle event is emitted as a structlog log entry keyed by
:class:`~loom.etl.executor.EventName`.  All fields are passed as
keyword arguments — never interpolated into the message string — so
processors (JSON, Datadog, …) receive fully structured data.

Data-flow enrichment
--------------------
``on_step_start`` always emits the resolved source refs / paths and the
target ref / path alongside the step name and backend.  This makes
every step's data flow visible in logs without any extra configuration::

    {
      "event": "step_start",
      "step": "BuildOrdersStaging",
      "sources": ["raw.orders", "raw.customers"],
      "target": "staging.orders",
      "write_mode": "replace_partitions",
      "backend": "polars",
      "run_id": "...",
      "step_run_id": "..."
    }

Slow-step warning
-----------------
Pass ``slow_step_threshold_ms`` to emit a ``WARNING``-level ``slow_step``
event whenever a step exceeds the threshold::

    StructlogRunObserver(slow_step_threshold_ms=60_000)

Configurable via ``observability.slow_step_threshold_ms`` in YAML.

Internal module — import from :mod:`loom.etl.executor.observer`.
"""

from __future__ import annotations

from typing import Any

import structlog

from loom.etl.executor.observer._events import EventName, RunContext, RunStatus

_log: Any = structlog.get_logger("loom.etl")


class StructlogRunObserver:
    """Observer that emits fully structured events via structlog.

    Every field is a keyword argument — the event key is an
    :class:`~loom.etl.executor.EventName` constant, never a raw string.
    ``correlation_id`` and ``attempt`` are included when present so retry
    attempts are traceable in log aggregators.

    ``on_step_start`` always includes resolved source refs and the target ref
    so data flow is visible without extra tooling.

    Added automatically by :meth:`~loom.etl.ETLRunner.from_yaml` when
    ``observability.log: true`` (the default).  Compatible with any
    structlog processor chain (JSON, console, Datadog…).

    Args:
        slow_step_threshold_ms: When set, emits a ``WARNING``-level
                                ``slow_step`` event whenever a step's
                                ``duration_ms`` exceeds this value.
                                ``None`` disables the warning (default).

    Example::

        import structlog
        structlog.configure(processors=[structlog.processors.JSONRenderer()])
        observer = StructlogRunObserver(slow_step_threshold_ms=30_000)
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
        sources = [_source_label(b.spec) for b in plan.source_bindings]
        _log.info(
            EventName.STEP_START,
            step=plan.step_type.__name__,
            sources=sources,
            target=_target_label(plan.target_binding.spec),
            write_mode=plan.target_binding.spec.mode,
            backend=str(plan.backend),
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _source_label(spec: Any) -> str:
    """Return a human-readable label for a source spec."""
    if spec.table_ref is not None:
        return str(spec.table_ref.ref)
    if getattr(spec, "temp_name", None) is not None:
        return f"temp:{spec.temp_name}"
    return str(spec.path) if spec.path else "?"


def _target_label(spec: Any) -> str:
    """Return a human-readable label for a target spec."""
    if spec.table_ref is not None:
        return str(spec.table_ref.ref)
    if getattr(spec, "temp_name", None) is not None:
        return f"temp:{spec.temp_name}"
    return str(spec.path) if spec.path else "?"
