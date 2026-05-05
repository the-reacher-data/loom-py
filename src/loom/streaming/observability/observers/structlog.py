"""Structured log observers for Kafka streaming operations."""

from __future__ import annotations

from typing import Any

import structlog

_kafka_log: Any = structlog.get_logger("loom.streaming.kafka")
_flow_log: Any = structlog.get_logger("loom.streaming.flow")


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
            _kafka_log.debug("kafka_produce", **fields)
            return
        _kafka_log.warning("kafka_produce", **fields)

    def on_consumed(self, topic: str, *, status: str = "success") -> None:
        """Log one consume operation."""
        fields = self._operation_fields("consume", topic, status)
        if status == "success":
            _kafka_log.debug("kafka_consume", **fields)
            return
        _kafka_log.warning("kafka_consume", **fields)

    def observe_encode(self, content_type: str, duration_seconds: float) -> None:
        """Log one envelope encode duration at debug level."""
        _kafka_log.debug(
            "kafka_encode",
            content_type=content_type,
            duration_ms=_duration_ms(duration_seconds),
        )

    def observe_decode(self, content_type: str, duration_seconds: float) -> None:
        """Log one envelope decode duration at debug level."""
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


class StructlogFlowObserver:
    """Observer that emits structured flow lifecycle events through structlog.

    Args:
        slow_node_threshold_ms: Optional threshold to emit ``slow_node``
            warnings. Defaults to ``None`` (disabled).
    """

    def __init__(self, *, slow_node_threshold_ms: int | None = None) -> None:
        self._slow_ms = slow_node_threshold_ms

    def on_flow_start(self, flow_name: str, *, node_count: int) -> None:
        """Log flow start at info level."""
        _flow_log.info("flow_start", flow=flow_name, node_count=node_count)

    def on_flow_end(self, flow_name: str, *, status: str, duration_ms: int) -> None:
        """Log flow end at info level."""
        _flow_log.info("flow_end", flow=flow_name, status=status, duration_ms=duration_ms)

    def on_node_start(self, flow_name: str, node_idx: int, *, node_type: str) -> None:
        """Log node start at debug level."""
        _flow_log.debug("node_start", flow=flow_name, node_idx=node_idx, node_type=node_type)

    def on_node_end(
        self,
        flow_name: str,
        node_idx: int,
        *,
        node_type: str,
        status: str,
        duration_ms: int,
    ) -> None:
        """Log node end at debug level, with slow-node warning if threshold exceeded."""
        _flow_log.debug(
            "node_end",
            flow=flow_name,
            node_idx=node_idx,
            node_type=node_type,
            status=status,
            duration_ms=duration_ms,
        )
        if self._slow_ms is not None and duration_ms > self._slow_ms:
            _flow_log.warning(
                "slow_node",
                flow=flow_name,
                node_idx=node_idx,
                node_type=node_type,
                duration_ms=duration_ms,
                threshold_ms=self._slow_ms,
            )

    def on_node_error(
        self, flow_name: str, node_idx: int, *, node_type: str, exc: Exception
    ) -> None:
        """Log node error at error level."""
        _flow_log.error(
            "node_error",
            flow=flow_name,
            node_idx=node_idx,
            node_type=node_type,
            error=repr(exc),
        )

    def on_collect_batch(
        self,
        flow_name: str,
        node_idx: int,
        *,
        node_type: str,
        batch_size: int,
        max_records: int,
        timeout_ms: int,
        reason: str,
    ) -> None:
        """Log one batch emission with its best-effort outcome label."""
        _flow_log.info(
            "collect_batch",
            flow=flow_name,
            node_idx=node_idx,
            node_type=node_type,
            batch_size=batch_size,
            max_records=max_records,
            timeout_ms=timeout_ms,
            reason=reason,
        )


def _duration_ms(duration_seconds: float) -> float:
    return round(duration_seconds * 1000, 3)


__all__ = ["StructlogFlowObserver", "StructlogKafkaObserver"]
