"""Shared observability test cases and helpers."""

from __future__ import annotations

from loom.core.model import LoomStruct
from loom.streaming import FromTopic, IntoTopic, Message, Process, RecordStep, StreamFlow


class DropItem(LoomStruct):
    """Payload dropped by unrouted error tests."""

    value: str


class DropBoomStep(RecordStep[DropItem, DropItem]):
    """Step that always fails to exercise unrouted error handling."""

    def execute(self, message: Message[DropItem], **kwargs: object) -> DropItem:
        raise ValueError("kaboom")


class RecordingFlowObserver:
    """Observer that records every callback for assertions."""

    def __init__(self) -> None:
        self.events: list[tuple[str, ...]] = []

    def on_flow_start(self, flow_name: str, *, node_count: int) -> None:
        self.events.append(("flow_start", flow_name, str(node_count)))

    def on_flow_end(self, flow_name: str, *, status: str, duration_ms: int) -> None:
        self.events.append(("flow_end", flow_name, status, str(duration_ms)))

    def on_node_start(self, flow_name: str, node_idx: int, *, node_type: str) -> None:
        self.events.append(("node_start", flow_name, str(node_idx), node_type))

    def on_node_end(
        self,
        flow_name: str,
        node_idx: int,
        *,
        node_type: str,
        status: str,
        duration_ms: int,
    ) -> None:
        self.events.append(("node_end", flow_name, str(node_idx), node_type, status))

    def on_node_error(
        self,
        flow_name: str,
        node_idx: int,
        *,
        node_type: str,
        exc: Exception,
    ) -> None:
        self.events.append(("node_error", flow_name, str(node_idx), node_type, repr(exc)))


class FailingFlowObserver:
    """Observer that always raises to test error isolation."""

    def on_flow_start(self, flow_name: str, *, node_count: int) -> None:
        raise RuntimeError("boom")

    def on_flow_end(self, flow_name: str, *, status: str, duration_ms: int) -> None:
        raise RuntimeError("boom")

    def on_node_start(self, flow_name: str, node_idx: int, *, node_type: str) -> None:
        raise RuntimeError("boom")

    def on_node_end(
        self,
        flow_name: str,
        node_idx: int,
        *,
        node_type: str,
        status: str,
        duration_ms: int,
    ) -> None:
        raise RuntimeError("boom")

    def on_node_error(
        self,
        flow_name: str,
        node_idx: int,
        *,
        node_type: str,
        exc: Exception,
    ) -> None:
        raise RuntimeError("boom")


def build_drop_flow() -> StreamFlow[DropItem, DropItem]:
    """Build a flow that fails with an unrouted task error."""
    return StreamFlow(
        name="boom_flow",
        source=FromTopic("items", payload=DropItem),
        process=Process(DropBoomStep()),
        output=IntoTopic("items.out", payload=DropItem),
    )
