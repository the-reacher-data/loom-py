"""Structured log observer for Kafka streaming operations."""

from __future__ import annotations

from typing import Any

import structlog

_log: Any = structlog.get_logger("loom.streaming.kafka")


class StructlogKafkaObserver:
    """Observer that emits structured Kafka events through structlog.

    Kafka topics can create high-cardinality logs in multi-tenant or
    dynamically routed deployments. This observer omits topic names by
    default and logs successful operations at debug level.

    Args:
        include_topic: Include Kafka topic names in log fields. Defaults to
            ``False`` to keep log cardinality bounded.
    """

    def __init__(self, *, include_topic: bool = False) -> None:
        self._include_topic = include_topic

    def on_produced(self, topic: str, *, status: str = "success") -> None:
        """Log one produce operation."""
        fields = self._operation_fields("produce", topic, status)
        if status == "success":
            _log.debug("kafka_produce", **fields)
            return
        _log.warning("kafka_produce", **fields)

    def on_consumed(self, topic: str, *, status: str = "success") -> None:
        """Log one consume operation."""
        fields = self._operation_fields("consume", topic, status)
        if status == "success":
            _log.debug("kafka_consume", **fields)
            return
        _log.warning("kafka_consume", **fields)

    def observe_encode(self, content_type: str, duration_seconds: float) -> None:
        """Log one envelope encode duration at debug level."""
        _log.debug(
            "kafka_encode",
            content_type=content_type,
            duration_ms=_duration_ms(duration_seconds),
        )

    def observe_decode(self, content_type: str, duration_seconds: float) -> None:
        """Log one envelope decode duration at debug level."""
        _log.debug(
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


__all__ = ["StructlogKafkaObserver"]
