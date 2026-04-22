"""Observer protocols for streaming transport operations."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class KafkaStreamingObserver(Protocol):
    """Observability contract for Kafka transport and message operations."""

    def on_produced(self, topic: str, *, status: str = "success") -> None:
        """Record one produce operation.

        Args:
            topic: Kafka topic name.
            status: Outcome label.
        """

    def on_consumed(self, topic: str, *, status: str = "success") -> None:
        """Record one consume operation.

        Args:
            topic: Kafka topic name.
            status: Outcome label.
        """

    def observe_encode(self, content_type: str, duration_seconds: float) -> None:
        """Observe one encode duration.

        Args:
            content_type: Wire content type label.
            duration_seconds: Encode duration in seconds.
        """

    def observe_decode(self, content_type: str, duration_seconds: float) -> None:
        """Observe one decode duration.

        Args:
            content_type: Wire content type label.
            duration_seconds: Decode duration in seconds.
        """


__all__ = ["KafkaStreamingObserver"]
