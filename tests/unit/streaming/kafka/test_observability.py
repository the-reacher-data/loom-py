from __future__ import annotations

from unittest.mock import Mock

import pytest
from prometheus_client import CollectorRegistry, generate_latest

from loom.prometheus import KafkaPrometheusMetrics
from loom.streaming.kafka import (
    KafkaStreamingObserver,
    NoopKafkaObserver,
    StructlogKafkaObserver,
)

pytestmark = pytest.mark.kafka


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


def _patch_kafka_log(
    monkeypatch: pytest.MonkeyPatch,
    log: Mock,
) -> None:
    """Patch the shared Kafka structlog logger used by the observer."""
    monkeypatch.setattr("loom.streaming.kafka._observability._kafka_log", log)


def test_kafka_prometheus_metrics_emit_counters_and_histograms(
    kafka_registry: CollectorRegistry,
) -> None:
    metrics = KafkaPrometheusMetrics(registry=kafka_registry)

    metrics.on_produced("orders")
    metrics.on_consumed("orders")
    metrics.observe_encode("application/x-loom-msgpack", 0.01)
    metrics.observe_decode("application/x-loom-msgpack", 0.02)

    text = generate_latest(kafka_registry).decode("utf-8")

    assert "loom_streaming_kafka_produced_total" in text
    assert "loom_streaming_kafka_consumed_total" in text
    assert "loom_streaming_kafka_encode_duration_seconds" in text
    assert "loom_streaming_kafka_decode_duration_seconds" in text


def test_noop_kafka_observer_matches_protocol() -> None:
    observer: KafkaStreamingObserver = NoopKafkaObserver()

    observer.on_produced("orders")
    observer.on_consumed("orders")
    observer.observe_encode("application/x-loom-msgpack", 0.01)
    observer.observe_decode("application/x-loom-msgpack", 0.02)


def test_structlog_kafka_observer_logs_success_at_debug_without_topic_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observer = StructlogKafkaObserver()

    log = Mock()
    _patch_kafka_log(monkeypatch, log)
    observer.on_produced("tenant-a.orders")

    log.debug.assert_called_once_with(
        "kafka_produce",
        operation="produce",
        status="success",
    )
    assert log.warning.call_count == 0


def test_structlog_kafka_observer_can_include_topic_explicitly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observer = StructlogKafkaObserver(include_topic=True)

    log = Mock()
    _patch_kafka_log(monkeypatch, log)
    observer.on_consumed("tenant-a.orders", status="decode_error")

    log.warning.assert_called_once_with(
        "kafka_consume",
        operation="consume",
        status="decode_error",
        topic="tenant-a.orders",
    )
    assert log.debug.call_count == 0


def test_structlog_kafka_observer_logs_codec_durations_at_debug(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observer = StructlogKafkaObserver()

    log = Mock()
    _patch_kafka_log(monkeypatch, log)
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
