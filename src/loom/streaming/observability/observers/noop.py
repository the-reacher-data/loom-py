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


class NoopFlowObserver:
    """Observer that does nothing for flow execution lifecycle events."""

    def on_flow_start(self, flow_name: str, *, node_count: int) -> None:
        """No-op."""

    def on_flow_end(self, flow_name: str, *, status: str, duration_ms: int) -> None:
        """No-op."""

    def on_node_start(self, flow_name: str, node_idx: int, *, node_type: str) -> None:
        """No-op."""

    def on_node_end(
        self, flow_name: str, node_idx: int, *, node_type: str, status: str, duration_ms: int
    ) -> None:
        """No-op."""

    def on_node_error(
        self, flow_name: str, node_idx: int, *, node_type: str, exc: Exception
    ) -> None:
        """No-op."""

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
        """No-op."""


__all__ = ["NoopFlowObserver", "NoopKafkaObserver"]
