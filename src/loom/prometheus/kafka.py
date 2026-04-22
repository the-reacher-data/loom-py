"""Prometheus metrics adapter for Kafka transport operations.

Optional module — requires ``prometheus-client``::

    pip install "loom-py[prometheus]"

Usage::

    from prometheus_client import CollectorRegistry
    from loom.prometheus import KafkaPrometheusMetrics

    registry = CollectorRegistry()
    metrics = KafkaPrometheusMetrics(registry=registry)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry, Counter, Histogram


class KafkaPrometheusMetrics:
    """Prometheus metrics recorder for Kafka transport operations.

    Records four instruments:

    - ``loom_streaming_kafka_produced_total``
    - ``loom_streaming_kafka_consumed_total``
    - ``loom_streaming_kafka_encode_duration_seconds``
    - ``loom_streaming_kafka_decode_duration_seconds``

    Args:
        registry: Optional ``CollectorRegistry``. Defaults to the global
            Prometheus registry when ``None``.

    Note:
        This class does not use any module-level mutable cache. When a custom
        registry is required for test isolation or multi-app setups, pass it
        explicitly.
    """

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self._produced_total = _build_produced_total(registry)
        self._consumed_total = _build_consumed_total(registry)
        self._encode_duration = _build_encode_duration(registry)
        self._decode_duration = _build_decode_duration(registry)

    def on_produced(self, topic: str, *, status: str = "success") -> None:
        """Record one produce operation.

        Args:
            topic: Kafka topic name.
            status: Operation outcome.
        """

        self._produced_total.labels(topic=topic, status=status).inc()

    def on_consumed(self, topic: str, *, status: str = "success") -> None:
        """Record one consume operation.

        Args:
            topic: Kafka topic name.
            status: Operation outcome.
        """

        self._consumed_total.labels(topic=topic, status=status).inc()

    def observe_encode(self, content_type: str, duration_seconds: float) -> None:
        """Observe one encode duration.

        Args:
            content_type: Wire content type label.
            duration_seconds: Encode duration in seconds.
        """

        self._encode_duration.labels(content_type=content_type).observe(duration_seconds)

    def observe_decode(self, content_type: str, duration_seconds: float) -> None:
        """Observe one decode duration.

        Args:
            content_type: Wire content type label.
            duration_seconds: Decode duration in seconds.
        """

        self._decode_duration.labels(content_type=content_type).observe(duration_seconds)


def _build_produced_total(registry: CollectorRegistry | None) -> Counter:
    from prometheus_client import Counter

    if registry is None:
        return Counter(
            "loom_streaming_kafka_produced_total",
            "Total Kafka records produced by Loom streaming.",
            ["topic", "status"],
        )
    return Counter(
        "loom_streaming_kafka_produced_total",
        "Total Kafka records produced by Loom streaming.",
        ["topic", "status"],
        registry=registry,
    )


def _build_consumed_total(registry: CollectorRegistry | None) -> Counter:
    from prometheus_client import Counter

    if registry is None:
        return Counter(
            "loom_streaming_kafka_consumed_total",
            "Total Kafka records consumed by Loom streaming.",
            ["topic", "status"],
        )
    return Counter(
        "loom_streaming_kafka_consumed_total",
        "Total Kafka records consumed by Loom streaming.",
        ["topic", "status"],
        registry=registry,
    )


def _build_encode_duration(registry: CollectorRegistry | None) -> Histogram:
    from prometheus_client import Histogram

    if registry is None:
        return Histogram(
            "loom_streaming_kafka_encode_duration_seconds",
            "Kafka envelope encode duration in seconds.",
            ["content_type"],
        )
    return Histogram(
        "loom_streaming_kafka_encode_duration_seconds",
        "Kafka envelope encode duration in seconds.",
        ["content_type"],
        registry=registry,
    )


def _build_decode_duration(registry: CollectorRegistry | None) -> Histogram:
    from prometheus_client import Histogram

    if registry is None:
        return Histogram(
            "loom_streaming_kafka_decode_duration_seconds",
            "Kafka envelope decode duration in seconds.",
            ["content_type"],
        )
    return Histogram(
        "loom_streaming_kafka_decode_duration_seconds",
        "Kafka envelope decode duration in seconds.",
        ["content_type"],
        registry=registry,
    )
