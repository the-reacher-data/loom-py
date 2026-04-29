"""Composite streaming observers."""

from __future__ import annotations

from collections.abc import Sequence

from loom.core.observability import notify_observers
from loom.streaming.observability.observers.protocol import (
    KafkaStreamingObserver,
    StreamingFlowObserver,
)


class CompositeKafkaObserver:
    """Fan-out Kafka events to multiple observers with error isolation.

    Args:
        observers: Sequence of observer instances implementing
            :class:`KafkaStreamingObserver`.
    """

    def __init__(self, observers: Sequence[KafkaStreamingObserver]) -> None:
        self._observers = tuple(observers)

    def on_produced(self, topic: str, *, status: str = "success") -> None:
        """Fan-out produce event to all observers."""
        notify_observers(self._observers, "on_produced", topic, status=status)

    def on_consumed(self, topic: str, *, status: str = "success") -> None:
        """Fan-out consume event to all observers."""
        notify_observers(self._observers, "on_consumed", topic, status=status)

    def observe_encode(self, content_type: str, duration_seconds: float) -> None:
        """Fan-out encode observation to all observers."""
        notify_observers(self._observers, "observe_encode", content_type, duration_seconds)

    def observe_decode(self, content_type: str, duration_seconds: float) -> None:
        """Fan-out decode observation to all observers."""
        notify_observers(self._observers, "observe_decode", content_type, duration_seconds)


class CompositeFlowObserver:
    """Fan-out flow lifecycle events to multiple observers with error isolation.

    Args:
        observers: Sequence of observer instances implementing
            :class:`StreamingFlowObserver`.
    """

    def __init__(self, observers: Sequence[StreamingFlowObserver]) -> None:
        self._observers = tuple(observers)

    @property
    def observers(self) -> tuple[StreamingFlowObserver, ...]:
        """Return the composed observers in wiring order."""
        return self._observers

    def on_flow_start(self, flow_name: str, *, node_count: int) -> None:
        """Fan-out flow start event to all observers."""
        notify_observers(self._observers, "on_flow_start", flow_name, node_count=node_count)

    def on_flow_end(self, flow_name: str, *, status: str, duration_ms: int) -> None:
        """Fan-out flow end event to all observers."""
        notify_observers(
            self._observers, "on_flow_end", flow_name, status=status, duration_ms=duration_ms
        )

    def on_node_start(self, flow_name: str, node_idx: int, *, node_type: str) -> None:
        """Fan-out node start event to all observers."""
        notify_observers(self._observers, "on_node_start", flow_name, node_idx, node_type=node_type)

    def on_node_end(
        self,
        flow_name: str,
        node_idx: int,
        *,
        node_type: str,
        status: str,
        duration_ms: int,
    ) -> None:
        """Fan-out node end event to all observers."""
        notify_observers(
            self._observers,
            "on_node_end",
            flow_name,
            node_idx,
            node_type=node_type,
            status=status,
            duration_ms=duration_ms,
        )

    def on_node_error(
        self, flow_name: str, node_idx: int, *, node_type: str, exc: Exception
    ) -> None:
        """Fan-out node error event to all observers."""
        notify_observers(
            self._observers, "on_node_error", flow_name, node_idx, node_type=node_type, exc=exc
        )


__all__ = ["CompositeFlowObserver", "CompositeKafkaObserver"]
