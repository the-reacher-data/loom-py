from __future__ import annotations

import pytest
from prometheus_client import CollectorRegistry, generate_latest

from loom.core.observability.event import LifecycleEvent, Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.prometheus import KafkaPrometheusMetrics

pytestmark = pytest.mark.kafka


def _make_obs(registry: CollectorRegistry) -> tuple[ObservabilityRuntime, KafkaPrometheusMetrics]:
    metrics = KafkaPrometheusMetrics(registry=registry)
    return ObservabilityRuntime([metrics]), metrics


class TestKafkaPrometheusMetrics:
    def test_produce_end_increments_produced_total(self, kafka_registry: CollectorRegistry) -> None:
        obs, _ = _make_obs(kafka_registry)

        obs.emit(
            LifecycleEvent.end(
                scope=Scope.TRANSPORT, name="kafka_produce", meta={"topic": "orders"}
            )
        )

        text = generate_latest(kafka_registry).decode()
        assert 'streaming_kafka_produced_total{status="success",topic="orders"}' in text

    def test_produce_error_increments_produced_total_with_delivery_error(
        self, kafka_registry: CollectorRegistry
    ) -> None:
        obs, _ = _make_obs(kafka_registry)

        obs.emit(
            LifecycleEvent.exception(
                scope=Scope.TRANSPORT,
                name="kafka_produce",
                error="timeout",
                meta={"topic": "orders"},
            )
        )

        text = generate_latest(kafka_registry).decode()
        assert 'streaming_kafka_produced_total{status="delivery_error",topic="orders"}' in text

    def test_consume_end_increments_consumed_total(self, kafka_registry: CollectorRegistry) -> None:
        obs, _ = _make_obs(kafka_registry)

        obs.emit(
            LifecycleEvent.end(
                scope=Scope.TRANSPORT, name="kafka_consume", meta={"topic": "orders"}
            )
        )

        text = generate_latest(kafka_registry).decode()
        assert 'streaming_kafka_consumed_total{status="success",topic="orders"}' in text

    def test_decode_error_increments_consumed_total_with_decode_error(
        self, kafka_registry: CollectorRegistry
    ) -> None:
        obs, _ = _make_obs(kafka_registry)

        obs.emit(
            LifecycleEvent.exception(
                scope=Scope.TRANSPORT,
                name="kafka_decode",
                error="bad bytes",
                meta={"topic": "orders"},
            )
        )

        text = generate_latest(kafka_registry).decode()
        assert 'streaming_kafka_consumed_total{status="decode_error",topic="orders"}' in text

    def test_encode_end_observes_encode_duration(self, kafka_registry: CollectorRegistry) -> None:
        obs, _ = _make_obs(kafka_registry)

        obs.emit(
            LifecycleEvent.end(
                scope=Scope.TRANSPORT,
                name="kafka_encode",
                duration_ms=10.0,
                meta={"content_type": "application/x-loom-msgpack"},
            )
        )

        text = generate_latest(kafka_registry).decode()
        assert "streaming_kafka_encode_duration_seconds" in text

    def test_decode_end_observes_decode_duration(self, kafka_registry: CollectorRegistry) -> None:
        obs, _ = _make_obs(kafka_registry)

        obs.emit(
            LifecycleEvent.end(
                scope=Scope.TRANSPORT,
                name="kafka_decode",
                duration_ms=20.0,
                meta={"content_type": "application/x-loom-msgpack"},
            )
        )

        text = generate_latest(kafka_registry).decode()
        assert "streaming_kafka_decode_duration_seconds" in text

    def test_non_transport_events_are_ignored(self, kafka_registry: CollectorRegistry) -> None:
        obs, _ = _make_obs(kafka_registry)

        obs.emit(LifecycleEvent.end(scope=Scope.NODE, name="transform", duration_ms=5.0))

        text = generate_latest(kafka_registry).decode()
        assert 'topic="' not in text
        assert 'content_type="' not in text

    def test_unknown_transport_name_is_ignored(self, kafka_registry: CollectorRegistry) -> None:
        obs, _ = _make_obs(kafka_registry)

        obs.emit(LifecycleEvent.end(scope=Scope.TRANSPORT, name="unknown_op", meta={"topic": "x"}))

        text = generate_latest(kafka_registry).decode()
        assert 'topic="x"' not in text

    def test_all_four_instruments_in_one_run(self, kafka_registry: CollectorRegistry) -> None:
        obs, _ = _make_obs(kafka_registry)

        obs.emit(
            LifecycleEvent.end(
                scope=Scope.TRANSPORT, name="kafka_produce", meta={"topic": "orders"}
            )
        )
        obs.emit(
            LifecycleEvent.end(
                scope=Scope.TRANSPORT, name="kafka_consume", meta={"topic": "orders"}
            )
        )
        obs.emit(
            LifecycleEvent.end(
                scope=Scope.TRANSPORT,
                name="kafka_encode",
                duration_ms=1.0,
                meta={"content_type": "application/x-loom-msgpack"},
            )
        )
        obs.emit(
            LifecycleEvent.end(
                scope=Scope.TRANSPORT,
                name="kafka_decode",
                duration_ms=2.0,
                meta={"content_type": "application/x-loom-msgpack"},
            )
        )

        text = generate_latest(kafka_registry).decode()
        assert "streaming_kafka_produced_total" in text
        assert "streaming_kafka_consumed_total" in text
        assert "streaming_kafka_encode_duration_seconds" in text
        assert "streaming_kafka_decode_duration_seconds" in text
