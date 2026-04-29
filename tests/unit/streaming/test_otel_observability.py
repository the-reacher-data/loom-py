"""Tests for streaming OpenTelemetry observability wiring."""

from __future__ import annotations

from collections.abc import Mapping

import pytest
from opentelemetry.trace import StatusCode

from loom.core.config.observability import OtelConfig
from loom.streaming.observability import (
    CompositeFlowObserver,
    OtelFlowObserver,
    StreamingObservabilityConfig,
    build_otel_observer,
)
from loom.streaming.observability.factory import make_flow_observers
from loom.streaming.observability.observers import otel as otel_observer
from loom.streaming.observability.observers.structlog import StructlogFlowObserver


def test_streaming_otel_config_defaults() -> None:
    """Streaming observability config should default to structured logs only."""
    cfg = StreamingObservabilityConfig()

    assert cfg.log is True
    assert cfg.otel is False
    assert cfg.otel_config is None
    assert cfg.slow_node_threshold_ms is None


def test_make_flow_observers_includes_otel_when_enabled() -> None:
    """The factory should include an OTEL observer when requested."""
    observer = make_flow_observers(StreamingObservabilityConfig(log=False, otel=True))

    assert isinstance(observer, OtelFlowObserver)


def test_make_flow_observers_combines_structlog_and_otel() -> None:
    """The factory should combine structlog and OTEL observers when both are enabled."""
    observer = make_flow_observers(StreamingObservabilityConfig(log=True, otel=True))

    assert isinstance(observer, CompositeFlowObserver)
    observers = observer.observers
    assert len(observers) == 2
    assert isinstance(observers[0], StructlogFlowObserver)
    assert isinstance(observers[1], OtelFlowObserver)


def test_build_otel_observer_without_config_returns_streaming_observer() -> None:
    """The OTEL builder should default to a valid streaming observer."""
    observer = build_otel_observer(None)

    assert isinstance(observer, OtelFlowObserver)


def test_otel_config_round_trips_through_msgspec() -> None:
    """The shared OTEL config object should be reusable from YAML."""
    cfg = OtelConfig(
        service_name="loom-streaming",
        tracer_name="loom.streaming",
        protocol="http/protobuf",
        endpoint="https://collector:4318/v1/traces",
        headers={"x-api-key": "token"},
        resource_attributes={"env": "test"},
        span_attributes={"team": "data"},
        exporter_kwargs={"timeout": 10},
        span_processor_kwargs={"max_export_batch_size": 128},
    )

    assert cfg.service_name == "loom-streaming"
    assert cfg.tracer_name == "loom.streaming"
    assert cfg.endpoint.endswith("/v1/traces")


class _FakeExporter:
    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs


def test_build_otel_exporter_for_http_forwards_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(otel_observer, "HttpOTLPSpanExporter", _FakeExporter)
    cfg = OtelConfig(
        service_name="loom-streaming",
        tracer_name="loom.streaming",
        protocol="http/protobuf",
        endpoint="https://collector:4318/v1/traces",
        headers={"x-api-key": "token"},
        exporter_kwargs={"timeout": 10},
    )

    exporter = otel_observer._build_exporter(cfg)

    assert isinstance(exporter, _FakeExporter)
    assert exporter.kwargs["endpoint"] == "https://collector:4318/v1/traces"
    assert exporter.kwargs["headers"] == {"x-api-key": "token"}
    assert exporter.kwargs["timeout"] == 10
    assert "insecure" not in exporter.kwargs


def test_build_otel_exporter_for_grpc_forwards_insecure_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(otel_observer, "GrpcOTLPSpanExporter", _FakeExporter)
    cfg = OtelConfig(
        service_name="loom-streaming",
        tracer_name="loom.streaming",
        protocol="grpc",
        endpoint="http://collector:4317",
        insecure=True,
    )

    exporter = otel_observer._build_exporter(cfg)

    assert isinstance(exporter, _FakeExporter)
    assert exporter.kwargs["endpoint"] == "http://collector:4317"
    assert exporter.kwargs["insecure"] is True


class _FakeSpan:
    def __init__(self, name: str) -> None:
        self.name = name
        self.attributes: dict[str, object] = {}
        self.ended = False
        self.status: tuple[StatusCode, str | None] | None = None
        self.exceptions: list[Exception] = []

    def set_attribute(self, key: str, value: object) -> None:
        self.attributes[key] = value

    def set_status(self, status: StatusCode, description: str | None = None) -> None:
        self.status = (status, description)

    def end(self) -> None:
        self.ended = True

    def record_exception(self, exc: Exception) -> None:
        self.exceptions.append(exc)


class _FakeTracer:
    def __init__(self) -> None:
        self.spans: list[_FakeSpan] = []

    def start_span(
        self,
        name: str,
        context: object | None = None,
        attributes: Mapping[str, object] | None = None,
    ) -> _FakeSpan:
        del context
        span = _FakeSpan(name)
        if attributes is not None:
            span.attributes.update(dict(attributes))
        self.spans.append(span)
        return span


class _FakeProvider:
    def __init__(self) -> None:
        self.force_flush_calls = 0

    def force_flush(self) -> None:
        self.force_flush_calls += 1


def test_otel_flow_observer_records_successful_flow_and_node_spans() -> None:
    tracer = _FakeTracer()
    provider = _FakeProvider()
    observer = OtelFlowObserver(
        tracer=tracer,
        tracer_provider=provider,
        static_attributes={"team": "data"},
    )

    observer.on_flow_start("orders", node_count=2)
    observer.on_node_start("orders", 0, node_type="Step")
    observer.on_node_end("orders", 0, node_type="Step", status="success", duration_ms=12)
    observer.on_flow_end("orders", status="success", duration_ms=50)

    assert tracer.spans[0].name == "loom.streaming.flow"
    assert tracer.spans[0].attributes["loom.flow"] == "orders"
    assert tracer.spans[0].attributes["loom.node_count"] == 2
    assert tracer.spans[0].attributes["team"] == "data"
    assert tracer.spans[0].ended is True
    assert tracer.spans[0].status == (StatusCode.OK, None)

    assert tracer.spans[1].name == "loom.streaming.node"
    assert tracer.spans[1].attributes["loom.node_idx"] == 0
    assert tracer.spans[1].attributes["loom.node_type"] == "Step"
    assert tracer.spans[1].ended is True
    assert tracer.spans[1].status == (StatusCode.OK, None)
    assert provider.force_flush_calls == 1


def test_otel_flow_observer_records_node_error_on_active_span() -> None:
    tracer = _FakeTracer()
    provider = _FakeProvider()
    observer = OtelFlowObserver(tracer=tracer, tracer_provider=provider)

    observer.on_flow_start("orders", node_count=1)
    observer.on_node_start("orders", 0, node_type="Step")
    observer.on_node_error("orders", 0, node_type="Step", exc=ValueError("boom"))
    observer.on_flow_end("orders", status="failed", duration_ms=50)

    assert tracer.spans[1].exceptions and isinstance(tracer.spans[1].exceptions[0], ValueError)
    assert tracer.spans[1].status == (StatusCode.ERROR, "ValueError('boom')")
    assert tracer.spans[1].ended is False
    assert provider.force_flush_calls == 1
