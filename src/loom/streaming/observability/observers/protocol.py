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


@runtime_checkable
class FlowLifecycleObserver(Protocol):
    """Observability contract for streaming flow lifecycle events."""

    def on_flow_start(self, flow_name: str, *, node_count: int) -> None:
        """Called when a flow begins execution.

        Args:
            flow_name: Stable name of the compiled flow.
            node_count: Number of compiled nodes in the plan.
        """

    def on_flow_end(
        self,
        flow_name: str,
        *,
        status: str,
        duration_ms: int,
    ) -> None:
        """Called when a flow completes or fails.

        Args:
            flow_name: Stable name of the compiled flow.
            status: Terminal status (``success`` or ``failed``).
            duration_ms: Wall-clock duration in milliseconds.
        """


@runtime_checkable
class NodeLifecycleObserver(Protocol):
    """Observability contract for streaming node lifecycle events."""

    def on_node_start(
        self,
        flow_name: str,
        node_idx: int,
        *,
        node_type: str,
    ) -> None:
        """Called before a compiled node executes.

        Args:
            flow_name: Stable name of the compiled flow.
            node_idx: Zero-based index of the node in the plan.
            node_type: Class name of the DSL node.
        """

    def on_node_end(
        self,
        flow_name: str,
        node_idx: int,
        *,
        node_type: str,
        status: str,
        duration_ms: int,
    ) -> None:
        """Called after a compiled node completes.

        Args:
            flow_name: Stable name of the compiled flow.
            node_idx: Zero-based index of the node in the plan.
            node_type: Class name of the DSL node.
            status: Outcome label (``success`` or ``failed``).
            duration_ms: Wall-clock duration in milliseconds.
        """

    def on_node_error(
        self,
        flow_name: str,
        node_idx: int,
        *,
        node_type: str,
        exc: Exception,
    ) -> None:
        """Called when a node raises an exception.

        Args:
            flow_name: Stable name of the compiled flow.
            node_idx: Zero-based index of the node in the plan.
            node_type: Class name of the DSL node.
            exc: The exception raised by the node.
        """


@runtime_checkable
class BatchCollectionObserver(Protocol):
    """Observability contract for batch collection summaries."""

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
        """Called when a batch is emitted by ``CollectBatch``.

        Args:
            flow_name: Stable name of the compiled flow.
            node_idx: Zero-based index of the node in the plan.
            node_type: Class name of the DSL node.
            batch_size: Number of messages emitted in the batch.
            max_records: Configured batch size threshold.
            timeout_ms: Configured batch timeout in milliseconds.
            reason: Best-effort outcome label, typically ``size`` or
                ``timeout_or_flush``.
        """


@runtime_checkable
class StreamingFlowObserver(
    FlowLifecycleObserver,
    NodeLifecycleObserver,
    BatchCollectionObserver,
    Protocol,
):
    """Composed streaming observability contract."""


__all__ = [
    "BatchCollectionObserver",
    "FlowLifecycleObserver",
    "KafkaStreamingObserver",
    "NodeLifecycleObserver",
    "StreamingFlowObserver",
]
