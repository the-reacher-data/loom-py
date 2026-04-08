"""OpenTelemetry trace observer for ETL lifecycle events.

Requires the ``etl-otel`` extra::

    pip install loom-kernel[etl-otel]

The observer emits one span per pipeline, process, and step run using the
active OTel tracer.  The SDK and exporter are configured externally — loom
only depends on ``opentelemetry-api``, which is a zero-config interface::

    OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
    OTEL_SERVICE_NAME=my-etl-service

Span hierarchy:

* ``loom.etl.pipeline`` — root span per pipeline run.
* ``loom.etl.process``  — child of pipeline span.
* ``loom.etl.step``     — child of pipeline span.  Process context is
  captured in the ``loom.run_id`` attribute; full process-level nesting
  is reserved for a future protocol extension.

Example::

    from loom.etl.observability.observers.otel import OtelRunObserver

    runner = ETLRunner.from_config(
        storage_config,
        observability=ObservabilityConfig(otel=True),
    )
"""

from __future__ import annotations

import threading
from typing import Any

from opentelemetry import trace
from opentelemetry.trace import StatusCode, set_span_in_context

from loom.etl.observability.observers.structlog import _target_label, _write_mode_label
from loom.etl.observability.records import RunContext, RunStatus

_tracer = trace.get_tracer("loom.etl")
_ATTR_RUN_ID = "loom.run_id"
_ATTR_STATUS = "loom.status"
_ATTR_DURATION_MS = "loom.duration_ms"


class OtelRunObserver:
    """Observer that emits OpenTelemetry spans for each ETL lifecycle event.

    Each pipeline, process, and step run produces one span.  Spans are
    ended and attributed in the corresponding ``on_*_end`` call.  Step
    errors are recorded via ``span.record_exception`` in ``on_step_error``.

    Thread-safe: parallel step groups call ``on_step_start`` / ``on_step_end``
    concurrently — all span dicts are guarded by a single lock.

    Example::

        from loom.etl.observability.observers.otel import OtelRunObserver

        observer = OtelRunObserver()
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pipeline_spans: dict[str, Any] = {}
        self._process_spans: dict[str, Any] = {}
        self._step_spans: dict[str, Any] = {}

    def on_pipeline_start(self, plan: Any, _params: Any, ctx: RunContext) -> None:
        span = _tracer.start_span(
            "loom.etl.pipeline",
            attributes={
                "loom.pipeline": plan.pipeline_type.__name__,
                _ATTR_RUN_ID: ctx.run_id,
                "loom.attempt": ctx.attempt,
            },
        )
        with self._lock:
            self._pipeline_spans[ctx.run_id] = span

    def on_pipeline_end(self, ctx: RunContext, status: RunStatus, duration_ms: int) -> None:
        with self._lock:
            span = self._pipeline_spans.pop(ctx.run_id, None)
        if span is None:
            return
        span.set_attribute(_ATTR_STATUS, str(status))
        span.set_attribute(_ATTR_DURATION_MS, duration_ms)
        span.set_status(StatusCode.OK if status == RunStatus.SUCCESS else StatusCode.ERROR)
        span.end()

    def on_process_start(self, plan: Any, ctx: RunContext, process_run_id: str) -> None:
        with self._lock:
            pipeline_span = self._pipeline_spans.get(ctx.run_id)
        parent_ctx = set_span_in_context(pipeline_span) if pipeline_span is not None else None
        span = _tracer.start_span(
            "loom.etl.process",
            context=parent_ctx,
            attributes={
                "loom.process": plan.process_type.__name__,
                "loom.process_run_id": process_run_id,
                _ATTR_RUN_ID: ctx.run_id,
            },
        )
        with self._lock:
            self._process_spans[process_run_id] = span

    def on_process_end(self, process_run_id: str, status: RunStatus, duration_ms: int) -> None:
        with self._lock:
            span = self._process_spans.pop(process_run_id, None)
        if span is None:
            return
        span.set_attribute(_ATTR_STATUS, str(status))
        span.set_attribute(_ATTR_DURATION_MS, duration_ms)
        span.set_status(StatusCode.OK if status == RunStatus.SUCCESS else StatusCode.ERROR)
        span.end()

    def on_step_start(self, plan: Any, ctx: RunContext, step_run_id: str) -> None:
        with self._lock:
            pipeline_span = self._pipeline_spans.get(ctx.run_id)
        parent_ctx = set_span_in_context(pipeline_span) if pipeline_span is not None else None
        span = _tracer.start_span(
            "loom.etl.step",
            context=parent_ctx,
            attributes={
                "loom.step": plan.step_type.__name__,
                "loom.step_run_id": step_run_id,
                _ATTR_RUN_ID: ctx.run_id,
                "loom.target": _target_label(plan.target_binding.spec),
                "loom.write_mode": _write_mode_label(plan.target_binding.spec),
            },
        )
        with self._lock:
            self._step_spans[step_run_id] = span

    def on_step_end(self, step_run_id: str, status: RunStatus, duration_ms: int) -> None:
        with self._lock:
            span = self._step_spans.pop(step_run_id, None)
        if span is None:
            return
        span.set_attribute(_ATTR_STATUS, str(status))
        span.set_attribute(_ATTR_DURATION_MS, duration_ms)
        span.set_status(StatusCode.OK if status == RunStatus.SUCCESS else StatusCode.ERROR)
        span.end()

    def on_step_error(self, step_run_id: str, exc: Exception) -> None:
        with self._lock:
            span = self._step_spans.get(step_run_id)
        if span is None:
            return
        span.record_exception(exc)
        span.set_status(StatusCode.ERROR, description=repr(exc))


__all__ = ["OtelRunObserver"]
