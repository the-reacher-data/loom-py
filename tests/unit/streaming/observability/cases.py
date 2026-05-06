"""Shared observability test cases and helpers."""

from __future__ import annotations

from loom.core.model import LoomStruct
from loom.core.observability.event import LifecycleEvent
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
        self.events: list[LifecycleEvent] = []

    def emit(self, event: LifecycleEvent) -> None:
        self.events.append(event)

    def on_event(self, event: LifecycleEvent) -> None:
        self.emit(event)


class FailingFlowObserver:
    """Observer that always raises to test error isolation."""

    def emit(self, event: LifecycleEvent) -> None:
        raise RuntimeError("boom")

    def on_event(self, event: LifecycleEvent) -> None:
        self.emit(event)


def build_drop_flow() -> StreamFlow[DropItem, DropItem]:
    """Build a flow that fails with an unrouted task error."""
    return StreamFlow(
        name="boom_flow",
        source=FromTopic("items", payload=DropItem),
        process=Process(DropBoomStep()),
        output=IntoTopic("items.out", payload=DropItem),
    )
