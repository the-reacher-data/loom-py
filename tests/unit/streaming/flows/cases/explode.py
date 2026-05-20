"""Explode flow-case builder."""

from __future__ import annotations

from typing import Any, ClassVar

from omegaconf import DictConfig

from loom.core.model import LoomStruct
from loom.streaming import (
    Explode,
    FromTopic,
    IntoTopic,
    Message,
    MessageMeta,
    Process,
    RecordStep,
    Router,
    StreamFlow,
)
from loom.streaming.nodes._protocols import Selector
from tests.unit.streaming.flows.cases.shared import (
    _ORDERS_RAW_TOPIC,
    StreamFlowCase,
    _build_case,
)


class StoreEvent(LoomStruct):
    """Input event expanded into typed sub-events."""

    store_id: str
    name: str
    language_codes: tuple[str, ...]


class StoreRow(LoomStruct):
    """Materialized store row."""

    store_id: str
    name: str


class LanguageRow(LoomStruct):
    """Materialized language row."""

    code: str


class RoutedRow(LoomStruct):
    """Common routed output produced by branch subprocesses."""

    source: str
    ref: str


class StoreEventExpander:
    """Expand a store event into store and language rows."""

    outputs: ClassVar[tuple[type, ...]] = (StoreRow, LanguageRow)

    @classmethod
    def expand(cls, event: StoreEvent) -> dict[type, list[Any]]:
        return {
            StoreRow: [StoreRow(store_id=event.store_id, name=event.name)],
            LanguageRow: [LanguageRow(code=code) for code in event.language_codes],
        }


class _TypeSelector(Selector[StoreEvent]):
    """Select the emitted sub-event type for type-keyed routing."""

    def select(self, message: Message[StoreEvent]) -> object:
        return type(message.payload)


class StoreRowRouteStep(RecordStep[StoreRow, RoutedRow]):
    """Convert a store row into a common routed output."""

    def execute(self, message: Message[StoreRow], **kwargs: object) -> RoutedRow:
        del kwargs
        return RoutedRow(source="store", ref=message.payload.store_id)


class LanguageRowRouteStep(RecordStep[LanguageRow, RoutedRow]):
    """Convert a language row into a common routed output."""

    def execute(self, message: Message[LanguageRow], **kwargs: object) -> RoutedRow:
        del kwargs
        return RoutedRow(source="language", ref=message.payload.code)


def build_explode_flow_case(config: DictConfig) -> StreamFlowCase:
    """Build a flow that explodes one event into typed branch outputs."""
    flow: StreamFlow[Any, Any] = StreamFlow(
        name="stores_explode",
        source=FromTopic(_ORDERS_RAW_TOPIC, payload=StoreEvent),
        process=Process(
            Explode(StoreEventExpander),
            Router.by(
                _TypeSelector(),
                {
                    StoreRow: Process(StoreRowRouteStep()),
                    LanguageRow: Process(LanguageRowRouteStep()),
                },
            ),
        ),
        output=IntoTopic("events.analytics", payload=RoutedRow),
    )
    return _build_case(
        flow=flow,
        config=config,
        input_messages=(
            _message(
                StoreEvent(
                    store_id="store-1",
                    name="North",
                    language_codes=("en", "es"),
                )
            ),
        ),
        expected_payloads=(
            RoutedRow(source="store", ref="store-1"),
            RoutedRow(source="language", ref="en"),
            RoutedRow(source="language", ref="es"),
        ),
    )


def _message(payload: StoreEvent) -> Message[StoreEvent]:
    return Message(
        payload=payload,
        meta=MessageMeta(message_id=payload.store_id),
    )
