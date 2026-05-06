"""Prometheus metrics adapter for Kafka transport operations.

Optional module — requires ``prometheus-client``::

    pip install "loom-py[prometheus]"

Usage::

    from prometheus_client import CollectorRegistry
    from loom.prometheus import KafkaPrometheusMetrics
    from loom.core.observability.runtime import ObservabilityRuntime

    registry = CollectorRegistry()
    metrics = KafkaPrometheusMetrics(registry=registry)
    obs = ObservabilityRuntime([metrics])
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from loom.core.observability.event import EventKind, LifecycleEvent, Scope

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry, Counter, Histogram


class KafkaPrometheusMetrics:
    """Prometheus metrics recorder for Kafka transport lifecycle events.

    Listens for :class:`~loom.core.observability.event.LifecycleEvent` with
    ``scope=TRANSPORT`` and records four instruments:

    - ``loom_streaming_kafka_produced_total`` — produce operations by topic/status.
    - ``loom_streaming_kafka_consumed_total`` — consume operations by topic/status.
    - ``loom_streaming_kafka_encode_duration_seconds`` — encode latency by content type.
    - ``loom_streaming_kafka_decode_duration_seconds`` — decode latency by content type.

    Wire this into an :class:`~loom.core.observability.runtime.ObservabilityRuntime`
    and pass the runtime to the Kafka client constructors via ``obs=``.

    Args:
        registry: Optional ``CollectorRegistry``. Defaults to the global
            Prometheus registry when ``None``.

    Example::

        metrics = KafkaPrometheusMetrics(registry=registry)
        obs = ObservabilityRuntime([metrics])
        consumer = KafkaConsumerClient(settings, obs=obs)
    """

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self._produced_total = _build_produced_total(registry)
        self._consumed_total = _build_consumed_total(registry)
        self._encode_duration = _build_encode_duration(registry)
        self._decode_duration = _build_decode_duration(registry)

    def on_event(self, event: LifecycleEvent) -> None:
        """Record one Kafka transport lifecycle event on Prometheus instruments.

        Only ``TRANSPORT`` scope events with names ``kafka_produce``,
        ``kafka_consume``, ``kafka_encode``, and ``kafka_decode`` are handled.
        All other events are ignored.

        Args:
            event: Lifecycle event from the runtime.
        """
        if event.scope is not Scope.TRANSPORT:
            return
        match event.name:
            case "kafka_produce":
                topic = str(event.meta.get("topic", "unknown"))
                status = "success" if event.kind is EventKind.END else "delivery_error"
                self._produced_total.labels(topic=topic, status=status).inc()
            case "kafka_consume":
                topic = str(event.meta.get("topic", "unknown"))
                self._consumed_total.labels(topic=topic, status="success").inc()
            case "kafka_encode":
                if event.duration_ms is not None:
                    ct = str(event.meta.get("content_type", "unknown"))
                    self._encode_duration.labels(content_type=ct).observe(event.duration_ms / 1000)
            case "kafka_decode":
                if event.kind is EventKind.ERROR:
                    topic = str(event.meta.get("topic", "unknown"))
                    self._consumed_total.labels(topic=topic, status="decode_error").inc()
                elif event.duration_ms is not None:
                    ct = str(event.meta.get("content_type", "unknown"))
                    self._decode_duration.labels(content_type=ct).observe(event.duration_ms / 1000)


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
