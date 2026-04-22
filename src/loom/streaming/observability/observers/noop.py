"""No-op streaming observers."""

from __future__ import annotations


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


__all__ = ["NoopKafkaObserver"]
