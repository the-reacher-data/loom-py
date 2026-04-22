"""Composite streaming observers."""

from __future__ import annotations

from collections.abc import Sequence

from loom.core.observability import notify_observers
from loom.streaming.observability.observers.protocol import KafkaStreamingObserver


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


__all__ = ["CompositeKafkaObserver"]
