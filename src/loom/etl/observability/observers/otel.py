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
from collections.abc import Mapping
from importlib.util import find_spec
from typing import Any

from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import StatusCode, set_span_in_context

from loom.etl.observability.config import OtelConfig
from loom.etl.observability.observers.structlog import _target_label, _write_mode_label
from loom.etl.observability.records import RunContext, RunStatus

_tracer = trace.get_tracer("loom.etl")
_ATTR_RUN_ID = "loom.run_id"
_ATTR_STATUS = "loom.status"
_ATTR_DURATION_MS = "loom.duration_ms"

if find_spec("opentelemetry.exporter.otlp.proto.grpc.trace_exporter") is not None:
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter as _GrpcOTLP

    _grpc_exporter: type[Any] | None = _GrpcOTLP
else:
    _grpc_exporter = None
GrpcOTLPSpanExporter: type[Any] | None = _grpc_exporter

if find_spec("opentelemetry.exporter.otlp.proto.http.trace_exporter") is not None:
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter as _HttpOTLP

    _http_exporter: type[Any] | None = _HttpOTLP
else:
    _http_exporter = None
HttpOTLPSpanExporter: type[Any] | None = _http_exporter


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

    def __init__(
        self,
        *,
        tracer: Any | None = None,
        static_attributes: Mapping[str, str] | None = None,
        tracer_provider: Any | None = None,
    ) -> None:
        self._tracer = tracer if tracer is not None else _tracer
        self._static_attributes = dict(static_attributes or {})
        self._tracer_provider = tracer_provider
        self._lock = threading.Lock()
        self._pipeline_spans: dict[str, Any] = {}
        self._process_spans: dict[str, Any] = {}
        self._step_spans: dict[str, Any] = {}

    def on_pipeline_start(self, plan: Any, _params: Any, ctx: RunContext) -> None:
        span = self._tracer.start_span(
            "loom.etl.pipeline",
            attributes={
                **self._static_attributes,
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
        provider = self._tracer_provider
        if provider is not None and hasattr(provider, "force_flush"):
            provider.force_flush()

    def on_process_start(self, plan: Any, ctx: RunContext, process_run_id: str) -> None:
        with self._lock:
            pipeline_span = self._pipeline_spans.get(ctx.run_id)
        parent_ctx = set_span_in_context(pipeline_span) if pipeline_span is not None else None
        span = self._tracer.start_span(
            "loom.etl.process",
            context=parent_ctx,
            attributes={
                **self._static_attributes,
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
        span = self._tracer.start_span(
            "loom.etl.step",
            context=parent_ctx,
            attributes={
                **self._static_attributes,
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


def build_otel_observer(config: OtelConfig | None) -> OtelRunObserver:
    """Build OTel observer from optional runtime configuration."""
    if config is None:
        return OtelRunObserver()
    config.validate()
    tracer, provider = _build_tracer(config)
    return OtelRunObserver(
        tracer=tracer,
        static_attributes=config.span_attributes,
        tracer_provider=provider,
    )


def _build_tracer(config: OtelConfig) -> tuple[Any, Any | None]:
    if not config.endpoint.strip():
        tracer = trace.get_tracer(
            config.tracer_name,
            config.tracer_version if config.tracer_version else None,
        )
        return tracer, None

    exporter = _build_exporter(config)
    resource_attrs = dict(config.resource_attributes)
    resource_attrs[SERVICE_NAME] = config.service_name
    provider = TracerProvider(resource=Resource.create(resource_attrs))
    provider.add_span_processor(BatchSpanProcessor(exporter, **config.span_processor_kwargs))
    tracer = provider.get_tracer(
        config.tracer_name,
        config.tracer_version if config.tracer_version else None,
    )
    return tracer, provider


def _build_exporter(config: OtelConfig) -> Any:
    kwargs = _exporter_kwargs(config)
    if config.protocol == "grpc":
        if GrpcOTLPSpanExporter is None:
            raise ValueError(
                "OTel protocol='grpc' requires package 'opentelemetry-exporter-otlp-proto-grpc'."
            )
        return GrpcOTLPSpanExporter(**kwargs)
    if HttpOTLPSpanExporter is None:
        raise ValueError(
            "OTel protocol='http/protobuf' requires package "
            "'opentelemetry-exporter-otlp-proto-http'."
        )
    return HttpOTLPSpanExporter(**kwargs)


def _exporter_kwargs(config: OtelConfig) -> dict[str, Any]:
    kwargs: dict[str, Any] = dict(config.exporter_kwargs)
    kwargs["endpoint"] = config.endpoint
    if config.headers:
        kwargs["headers"] = dict(config.headers)
    if config.protocol == "grpc":
        kwargs["insecure"] = config.insecure
    return kwargs


__all__ = ["OtelRunObserver", "build_otel_observer"]
