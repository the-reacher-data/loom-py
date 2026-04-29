"""Shared flow-case data and helpers for streaming tests."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from omegaconf import DictConfig

from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.streaming import Message, MessageMeta, RecordStep

_KAFKA_BROKER = "localhost:9092"
_ORDERS_RAW_TOPIC = "orders.raw"
_ORDERS_VALIDATED_TOPIC = "orders.validated"
_ORDERS_ROUTED_TOPIC = "orders.routed"
_ORDERS_PRICED_TOPIC = "orders.priced"
_ORDERS_PRICED_BATCH_SCOPE_TOPIC = "orders.priced.batch_scope"
_ORDERS_SCORED_TOPIC = "orders.scored"
_ORDERS_FORK_VIP_TOPIC = "orders.fork.vip"
_ORDERS_FORK_STANDARD_TOPIC = "orders.fork.standard"


class OrderPlaced(LoomStruct):
    order_id: str
    amount: int
    segment: str = "standard"


class RoutedOrder(LoomStruct):
    order_id: str
    lane: str


class PricedOrder(LoomStruct):
    order_id: str
    price_band: str
    client_id: int


class RiskScoredOrder(LoomStruct):
    order_id: str
    risk_band: str


class ValidatedOrder(LoomStruct):
    order_id: str
    accepted: bool


class ValidateOrder(RecordStep[OrderPlaced, ValidatedOrder]):
    def execute(self, message: Message[OrderPlaced], **kwargs: object) -> ValidatedOrder:
        del kwargs
        return ValidatedOrder(
            order_id=message.payload.order_id,
            accepted=message.payload.amount > 0,
        )


class MarkVipOrder(RecordStep[OrderPlaced, RoutedOrder]):
    def execute(self, message: Message[OrderPlaced], **kwargs: object) -> RoutedOrder:
        del kwargs
        return RoutedOrder(order_id=message.payload.order_id, lane="vip")


class MarkStandardOrder(RecordStep[OrderPlaced, RoutedOrder]):
    def execute(self, message: Message[OrderPlaced], **kwargs: object) -> RoutedOrder:
        del kwargs
        return RoutedOrder(order_id=message.payload.order_id, lane="standard")


class MarkManualOrder(RecordStep[OrderPlaced, RoutedOrder]):
    def execute(self, message: Message[OrderPlaced], **kwargs: object) -> RoutedOrder:
        del kwargs
        return RoutedOrder(order_id=message.payload.order_id, lane="manual")


class PriceOrder(RecordStep[OrderPlaced, PricedOrder]):
    def execute(
        self,
        message: Message[OrderPlaced],
        *,
        client: FakePricingClient,
    ) -> PricedOrder:
        if not isinstance(client, FakePricingClient):
            raise TypeError("client must be FakePricingClient.")
        return PricedOrder(
            order_id=message.payload.order_id,
            price_band=client.price_band(message.payload.amount),
            client_id=client.client_id,
        )


class ScoreRiskAsync(RecordStep[OrderPlaced, RiskScoredOrder]):
    async def execute(
        self,
        message: Message[OrderPlaced],
        *,
        client: FakeRiskClient,
    ) -> RiskScoredOrder:
        if not isinstance(client, FakeRiskClient):
            raise TypeError("client must be FakeRiskClient.")
        risk_band = await client.risk_band(message.payload.amount)
        return RiskScoredOrder(order_id=message.payload.order_id, risk_band=risk_band)


class FakePricingClient:
    def __init__(self, events: ResourceEvents, client_id: int) -> None:
        self._events = events
        self.client_id = client_id

    def __enter__(self) -> FakePricingClient:
        self._events.record_opened(self.client_id)
        return self

    def __exit__(self, *args: object) -> None:
        self._events.record_closed(self.client_id)
        return None

    def price_band(self, amount: int) -> str:
        return "high" if amount >= 100 else "low"


class FakeRiskClient:
    async def __aenter__(self) -> FakeRiskClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        await asyncio.sleep(0)
        return None

    async def risk_band(self, amount: int) -> str:
        return "high" if amount >= 100 else "low"


@dataclass(frozen=True)
class StreamFlowCase:
    """Reusable public-DSL flow case for compiler and adapter tests."""

    flow: Any
    config: DictConfig
    input_messages: tuple[Message[Any], ...]
    expected_payloads: tuple[LoomStruct | LoomFrozenStruct, ...]
    resource_events: ResourceEvents | None = None


@dataclass
class ResourceEvents:
    """Track fake context-manager lifecycle events for flow examples."""

    opened: list[int] = field(default_factory=list)
    closed: list[int] = field(default_factory=list)
    _next_client_id: int = 0

    def record_opened(self, client_id: int) -> None:
        """Record one opened client identifier."""
        self.opened.append(client_id)

    def record_closed(self, client_id: int) -> None:
        """Record one closed client identifier."""
        self.closed.append(client_id)

    def create_pricing_client(self) -> FakePricingClient:
        """Create a fake pricing client with a stable test identifier."""
        self._next_client_id += 1
        return FakePricingClient(self, self._next_client_id)

    def snapshot(self) -> tuple[tuple[int, ...], tuple[int, ...]]:
        """Return stable lifecycle snapshots for assertions."""
        return tuple(self.opened), tuple(self.closed)


def _build_case(
    *,
    flow: Any,
    config: DictConfig,
    input_messages: tuple[Message[Any], ...],
    expected_payloads: tuple[LoomStruct | LoomFrozenStruct, ...],
    resource_events: ResourceEvents | None = None,
) -> StreamFlowCase:
    """Assemble one reusable streaming flow case."""
    return StreamFlowCase(
        flow=flow,
        config=config,
        input_messages=input_messages,
        expected_payloads=expected_payloads,
        resource_events=resource_events,
    )


def _message(payload: OrderPlaced) -> Message[OrderPlaced]:
    return Message(
        payload=payload,
        meta=MessageMeta(message_id=payload.order_id),
    )


def _routed_order_message(order_id: str, lane: str) -> Message[RoutedOrder]:
    return Message(
        payload=RoutedOrder(order_id=order_id, lane=lane),
        meta=MessageMeta(message_id=order_id),
    )
