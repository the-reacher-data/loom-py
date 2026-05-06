"""OpenTelemetry lifecycle observer — emits spans and optional log correlation."""

from __future__ import annotations

import threading
from collections.abc import Callable, MutableMapping
from typing import Any

from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import StatusCode, set_span_in_context

from loom.core.config.observability import OtelConfig
from loom.core.observability.event import EventKind, LifecycleEvent
from loom.core.observability.topology import ROOT_SCOPES, span_parent_key

_grpc_exporter_cls: type[Any] | None
try:
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
        OTLPSpanExporter as _GrpcExporter,
    )
except ImportError:
    _grpc_exporter_cls = None
else:
    _grpc_exporter_cls = _GrpcExporter

_http_exporter_cls: type[Any] | None
try:
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
        OTLPSpanExporter as _HttpExporter,
    )
except ImportError:
    _http_exporter_cls = None
else:
    _http_exporter_cls = _HttpExporter


class OtelLifecycleObserver:
    """Lifecycle observer that emits OpenTelemetry spans for every ``span()`` call.

    Each ``START`` event opens a span keyed by ``scope:name:trace_id``. The
    matching ``END`` or ``ERROR`` event closes it. Concurrent spans with
    different ``trace_id`` values are tracked independently.

    When the runtime enables log export, it installs a structlog processor
    that adds the active OTEL trace and span IDs to every log entry.

    Args:
        config: OTLP exporter configuration.
    """

    def __init__(self, config: OtelConfig) -> None:
        config.validate()
        self._tracer, self._provider = _build_tracer(config)
        self._spans = _SpanRegistry()

    def on_event(self, event: LifecycleEvent) -> None:
        """Handle one lifecycle event.

        Args:
            event: Lifecycle event from the runtime.
        """
        match event.kind:
            case EventKind.START:
                self._open_span(event)
            case EventKind.END:
                self._close_span(event, ok=True)
            case EventKind.ERROR:
                self._close_span(event, ok=False)
            case _:
                pass

    def _open_span(self, event: LifecycleEvent) -> None:
        attrs = event.otel_attributes()

        parent_span = self._spans.get(span_parent_key(event.scope, event.trace_id))
        parent_ctx = set_span_in_context(parent_span) if parent_span is not None else None

        span = self._tracer.start_span(
            event.otel_span_name(),
            context=parent_ctx,
            attributes=attrs,
        )
        self._spans.put(_span_key(event), span)

    def _close_span(self, event: LifecycleEvent, *, ok: bool) -> None:
        span = self._spans.pop(_span_key(event))
        if span is None:
            return
        if event.duration_ms is not None:
            span.set_attribute("duration_ms", event.duration_ms)
        if ok:
            span.set_status(StatusCode.OK)
        else:
            span.set_status(StatusCode.ERROR, event.error or "")
        span.end()
        if (
            event.scope in ROOT_SCOPES
            and self._provider is not None
            and hasattr(self._provider, "force_flush")
        ):
            self._provider.force_flush()


class _SpanRegistry:
    """Thread-safe in-memory registry mapping span keys to active spans."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._spans: dict[str, Any] = {}

    def put(self, key: str, span: Any) -> None:
        with self._lock:
            self._spans[key] = span

    def get(self, key: str) -> Any | None:
        with self._lock:
            return self._spans.get(key)

    def pop(self, key: str) -> Any | None:
        with self._lock:
            return self._spans.pop(key, None)


def _span_key(event: LifecycleEvent) -> str:
    return f"{event.scope}:{event.name}:{event.trace_id or ''}"


def _build_tracer(config: OtelConfig) -> tuple[Any, Any | None]:
    if not config.endpoint.strip():
        return trace.get_tracer(config.tracer_name, config.tracer_version or None), None

    exporter = _build_exporter(config)
    resource_attrs = {SERVICE_NAME: config.service_name, **config.resource_attributes}
    provider = TracerProvider(resource=Resource.create(resource_attrs))
    provider.add_span_processor(BatchSpanProcessor(exporter, **config.span_processor_kwargs))
    tracer = provider.get_tracer(config.tracer_name, config.tracer_version or None)
    return tracer, provider


def _build_exporter(config: OtelConfig) -> Any:
    kwargs: dict[str, Any] = {"endpoint": config.endpoint, **config.exporter_kwargs}
    if config.headers:
        kwargs["headers"] = dict(config.headers)
    if config.protocol == "grpc":
        if _grpc_exporter_cls is None:
            raise ValueError(
                "OTel protocol='grpc' requires 'opentelemetry-exporter-otlp-proto-grpc'."
            )
        kwargs["insecure"] = config.insecure
        return _grpc_exporter_cls(**kwargs)
    if _http_exporter_cls is None:
        raise ValueError(
            "OTel protocol='http/protobuf' requires 'opentelemetry-exporter-otlp-proto-http'."
        )
    return _http_exporter_cls(**kwargs)


def build_log_correlation_processor() -> Callable[
    [Any, str, MutableMapping[str, Any]],
    MutableMapping[str, Any],
]:
    """Build a structlog processor that adds the active OTEL span IDs.

    Returns:
        A processor function compatible with ``structlog``.
    """

    def _processor(
        logger: Any,
        method: str,
        event_dict: MutableMapping[str, Any],
    ) -> MutableMapping[str, Any]:
        span = trace.get_current_span()
        if span.is_recording():
            ctx = span.get_span_context()
            event_dict.setdefault("otel_trace_id", format(ctx.trace_id, "032x"))
            event_dict.setdefault("otel_span_id", format(ctx.span_id, "016x"))
        return event_dict

    return _processor


__all__ = ["OtelLifecycleObserver", "build_log_correlation_processor"]
