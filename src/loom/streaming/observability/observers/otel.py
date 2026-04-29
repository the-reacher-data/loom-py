"""OpenTelemetry trace observer for streaming flow lifecycle events."""

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

from loom.core.config.observability import OtelConfig

_ATTR_FLOW = "loom.flow"
_ATTR_NODE_IDX = "loom.node_idx"
_ATTR_NODE_TYPE = "loom.node_type"
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


class OtelFlowObserver:
    """Observer that emits OpenTelemetry spans for streaming lifecycle events."""

    def __init__(
        self,
        *,
        tracer: Any | None = None,
        static_attributes: Mapping[str, str] | None = None,
        tracer_provider: Any | None = None,
    ) -> None:
        self._tracer = tracer or trace.get_tracer("loom.streaming")
        self._static_attributes = dict(static_attributes or {})
        self._tracer_provider = tracer_provider
        self._flow_spans = _SpanRegistry()
        self._node_spans = _SpanRegistry()

    def on_flow_start(self, flow_name: str, *, node_count: int) -> None:
        """Start one flow span."""
        span = self._tracer.start_span(
            "loom.streaming.flow",
            attributes={
                **self._static_attributes,
                _ATTR_FLOW: flow_name,
                "loom.node_count": node_count,
            },
        )
        self._flow_spans.put(flow_name, span)

    def on_flow_end(self, flow_name: str, *, status: str, duration_ms: int) -> None:
        """End one flow span."""
        span = self._flow_spans.pop(flow_name)
        if span is None:
            return
        span.set_attribute(_ATTR_STATUS, status)
        span.set_attribute(_ATTR_DURATION_MS, duration_ms)
        span.set_status(StatusCode.OK if status == "success" else StatusCode.ERROR)
        span.end()
        provider = self._tracer_provider
        if provider is not None and hasattr(provider, "force_flush"):
            provider.force_flush()

    def on_node_start(self, flow_name: str, node_idx: int, *, node_type: str) -> None:
        """Start one node span under the flow span when available."""
        flow_span = self._flow_spans.get(flow_name)
        parent_ctx = set_span_in_context(flow_span) if flow_span is not None else None
        span = self._tracer.start_span(
            "loom.streaming.node",
            context=parent_ctx,
            attributes={
                **self._static_attributes,
                _ATTR_FLOW: flow_name,
                _ATTR_NODE_IDX: node_idx,
                _ATTR_NODE_TYPE: node_type,
            },
        )
        self._node_spans.put(_node_key(flow_name, node_idx), span)

    def on_node_end(
        self,
        flow_name: str,
        node_idx: int,
        *,
        node_type: str,
        status: str,
        duration_ms: int,
    ) -> None:
        """End one node span."""
        span = self._node_spans.pop(_node_key(flow_name, node_idx))
        if span is None:
            return
        span.set_attribute(_ATTR_NODE_TYPE, node_type)
        span.set_attribute(_ATTR_STATUS, status)
        span.set_attribute(_ATTR_DURATION_MS, duration_ms)
        span.set_status(StatusCode.OK if status == "success" else StatusCode.ERROR)
        span.end()

    def on_node_error(
        self,
        flow_name: str,
        node_idx: int,
        *,
        node_type: str,
        exc: Exception,
    ) -> None:
        """Record one node error on the active node span and close it."""
        span = self._node_spans.pop(_node_key(flow_name, node_idx))
        if span is None:
            return
        span.set_attribute(_ATTR_NODE_TYPE, node_type)
        span.record_exception(exc)
        span.set_status(StatusCode.ERROR, description=repr(exc))
        span.end()


class _SpanRegistry:
    """Thread-safe in-memory span registry."""

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


def build_otel_observer(config: OtelConfig | None) -> OtelFlowObserver:
    """Build a streaming OTEL observer from optional runtime config."""
    if config is None:
        return OtelFlowObserver()
    config.validate()
    tracer, provider = _build_tracer(config)
    return OtelFlowObserver(
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


def _node_key(flow_name: str, node_idx: int) -> str:
    return f"{flow_name}:{node_idx}"


__all__ = ["OtelFlowObserver", "build_otel_observer"]
