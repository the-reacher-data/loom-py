"""Kafka transport observers for streaming runtimes."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

import structlog

_kafka_log: Any = structlog.get_logger("loom.streaming.kafka")


@runtime_checkable
class KafkaStreamingObserver(Protocol):
    """Observability contract for Kafka transport and message operations."""

    def on_produced(self, topic: str, *, status: str = "success") -> None:
        """Record one produce operation."""

    def on_consumed(self, topic: str, *, status: str = "success") -> None:
        """Record one consume operation."""

    def observe_encode(self, content_type: str, duration_seconds: float) -> None:
        """Observe one encode duration."""

    def observe_decode(self, content_type: str, duration_seconds: float) -> None:
        """Observe one decode duration."""


class NoopKafkaObserver:
    """Observer that does nothing when observability is disabled."""

    def on_produced(self, topic: str, *, status: str = "success") -> None:
        """No-op."""

    def on_consumed(self, topic: str, *, status: str = "success") -> None:
        """No-op."""

    def observe_encode(self, content_type: str, duration_seconds: float) -> None:
        """No-op."""

    def observe_decode(self, content_type: str, duration_seconds: float) -> None:
        """No-op."""


class StructlogKafkaObserver:
    """Observer that emits structured Kafka events through structlog."""

    def __init__(self, *, include_topic: bool = False) -> None:
        self._include_topic = include_topic

    def on_produced(self, topic: str, *, status: str = "success") -> None:
        fields = self._operation_fields("produce", topic, status)
        if status == "success":
            _kafka_log.debug("kafka_produce", **fields)
            return
        _kafka_log.warning("kafka_produce", **fields)

    def on_consumed(self, topic: str, *, status: str = "success") -> None:
        fields = self._operation_fields("consume", topic, status)
        if status == "success":
            _kafka_log.debug("kafka_consume", **fields)
            return
        _kafka_log.warning("kafka_consume", **fields)

    def observe_encode(self, content_type: str, duration_seconds: float) -> None:
        _kafka_log.debug(
            "kafka_encode",
            content_type=content_type,
            duration_ms=_duration_ms(duration_seconds),
        )

    def observe_decode(self, content_type: str, duration_seconds: float) -> None:
        _kafka_log.debug(
            "kafka_decode",
            content_type=content_type,
            duration_ms=_duration_ms(duration_seconds),
        )

    def _operation_fields(self, operation: str, topic: str, status: str) -> dict[str, str]:
        fields = {"operation": operation, "status": status}
        if self._include_topic:
            fields["topic"] = topic
        return fields


def _duration_ms(duration_seconds: float) -> float:
    return round(duration_seconds * 1000, 3)


__all__ = [
    "KafkaStreamingObserver",
    "NoopKafkaObserver",
    "StructlogKafkaObserver",
]
