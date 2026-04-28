"""Tests for streaming OpenTelemetry observability wiring."""

from __future__ import annotations

from loom.core.config.observability import OtelConfig
from loom.streaming.observability import (
    CompositeFlowObserver,
    OtelFlowObserver,
    StreamingObservabilityConfig,
    build_otel_observer,
)
from loom.streaming.observability.factory import make_flow_observers
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
    observers = observer._observers
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
