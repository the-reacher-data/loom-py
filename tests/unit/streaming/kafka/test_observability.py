from __future__ import annotations

from unittest.mock import patch

from prometheus_client import CollectorRegistry, generate_latest

from loom.prometheus import KafkaPrometheusMetrics
from loom.streaming.kafka import (
    CompositeKafkaObserver,
    KafkaStreamingObserver,
    NoopKafkaObserver,
    StructlogKafkaObserver,
)


class _RecordingKafkaObserver:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, str]] = []
        self.durations: list[tuple[str, str, float]] = []

    def on_produced(self, topic: str, *, status: str = "success") -> None:
        self.events.append(("produced", topic, status))

    def on_consumed(self, topic: str, *, status: str = "success") -> None:
        self.events.append(("consumed", topic, status))

    def observe_encode(self, content_type: str, duration_seconds: float) -> None:
        self.durations.append(("encode", content_type, duration_seconds))

    def observe_decode(self, content_type: str, duration_seconds: float) -> None:
        self.durations.append(("decode", content_type, duration_seconds))


class _FailingKafkaObserver:
    def on_produced(self, topic: str, *, status: str = "success") -> None:
        raise RuntimeError("produce failed")

    def on_consumed(self, topic: str, *, status: str = "success") -> None:
        raise RuntimeError("consume failed")

    def observe_encode(self, content_type: str, duration_seconds: float) -> None:
        raise RuntimeError("encode failed")

    def observe_decode(self, content_type: str, duration_seconds: float) -> None:
        raise RuntimeError("decode failed")


def test_kafka_prometheus_metrics_emit_counters_and_histograms() -> None:
    registry = CollectorRegistry()
    metrics = KafkaPrometheusMetrics(registry=registry)

    metrics.on_produced("orders")
    metrics.on_consumed("orders")
    metrics.observe_encode("application/x-loom-msgpack", 0.01)
    metrics.observe_decode("application/x-loom-msgpack", 0.02)

    text = generate_latest(registry).decode("utf-8")

    assert "loom_streaming_kafka_produced_total" in text
    assert "loom_streaming_kafka_consumed_total" in text
    assert "loom_streaming_kafka_encode_duration_seconds" in text
    assert "loom_streaming_kafka_decode_duration_seconds" in text


def test_composite_kafka_observer_fans_out_and_isolates_errors() -> None:
    first = _RecordingKafkaObserver()
    second = _RecordingKafkaObserver()
    composite = CompositeKafkaObserver([first, _FailingKafkaObserver(), second])

    composite.on_produced("orders")
    composite.on_consumed("orders", status="decode_error")
    composite.observe_encode("application/x-loom-msgpack", 0.01)
    composite.observe_decode("application/x-loom-msgpack", 0.02)

    assert first.events == [
        ("produced", "orders", "success"),
        ("consumed", "orders", "decode_error"),
    ]
    assert second.events == first.events
    assert first.durations == [
        ("encode", "application/x-loom-msgpack", 0.01),
        ("decode", "application/x-loom-msgpack", 0.02),
    ]
    assert second.durations == first.durations


def test_noop_kafka_observer_matches_protocol() -> None:
    observer: KafkaStreamingObserver = NoopKafkaObserver()

    observer.on_produced("orders")
    observer.on_consumed("orders")
    observer.observe_encode("application/x-loom-msgpack", 0.01)
    observer.observe_decode("application/x-loom-msgpack", 0.02)


def test_structlog_kafka_observer_logs_success_at_debug_without_topic_by_default() -> None:
    observer = StructlogKafkaObserver()

    with patch("loom.streaming.observability.observers.structlog._log") as log:
        observer.on_produced("tenant-a.orders")

    log.debug.assert_called_once_with(
        "kafka_produce",
        operation="produce",
        status="success",
    )
    assert log.warning.call_count == 0


def test_structlog_kafka_observer_can_include_topic_explicitly() -> None:
    observer = StructlogKafkaObserver(include_topic=True)

    with patch("loom.streaming.observability.observers.structlog._log") as log:
        observer.on_consumed("tenant-a.orders", status="decode_error")

    log.warning.assert_called_once_with(
        "kafka_consume",
        operation="consume",
        status="decode_error",
        topic="tenant-a.orders",
    )
    assert log.debug.call_count == 0


def test_structlog_kafka_observer_logs_codec_durations_at_debug() -> None:
    observer = StructlogKafkaObserver()

    with patch("loom.streaming.observability.observers.structlog._log") as log:
        observer.observe_encode("application/x-loom-msgpack", 0.0012345)
        observer.observe_decode("application/x-loom-msgpack", 0.002)

    log.debug.assert_any_call(
        "kafka_encode",
        content_type="application/x-loom-msgpack",
        duration_ms=1.234,
    )
    log.debug.assert_any_call(
        "kafka_decode",
        content_type="application/x-loom-msgpack",
        duration_ms=2.0,
    )
