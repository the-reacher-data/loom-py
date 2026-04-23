"""Reusable public-DSL flow cases for streaming tests."""

from __future__ import annotations

from collections.abc import Awaitable
from dataclasses import dataclass, field
from typing import Any

from omegaconf import DictConfig, OmegaConf

from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.streaming import (
    CollectBatch,
    ContextFactory,
    ForEach,
    FromTopic,
    IntoTopic,
    Message,
    MessageMeta,
    Process,
    ResourceScope,
    Router,
    StreamFlow,
    Task,
    With,
    WithAsync,
    msg,
)


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


class ValidateOrder(Task[OrderPlaced, ValidatedOrder]):
    def execute(self, message: Message[OrderPlaced], **kwargs: object) -> ValidatedOrder:
        return ValidatedOrder(
            order_id=message.payload.order_id,
            accepted=message.payload.amount > 0,
        )


class MarkVipOrder(Task[OrderPlaced, RoutedOrder]):
    def execute(self, message: Message[OrderPlaced], **kwargs: object) -> RoutedOrder:
        return RoutedOrder(order_id=message.payload.order_id, lane="vip")


class MarkStandardOrder(Task[OrderPlaced, RoutedOrder]):
    def execute(self, message: Message[OrderPlaced], **kwargs: object) -> RoutedOrder:
        return RoutedOrder(order_id=message.payload.order_id, lane="standard")


class MarkManualOrder(Task[OrderPlaced, RoutedOrder]):
    def execute(self, message: Message[OrderPlaced], **kwargs: object) -> RoutedOrder:
        return RoutedOrder(order_id=message.payload.order_id, lane="manual")


class PriceOrder(Task[OrderPlaced, PricedOrder]):
    def execute(self, message: Message[OrderPlaced], **kwargs: object) -> PricedOrder:
        client = kwargs["client"]
        if not isinstance(client, FakePricingClient):
            raise TypeError("client must be FakePricingClient.")
        return PricedOrder(
            order_id=message.payload.order_id,
            price_band=client.price_band(message.payload.amount),
            client_id=client.client_id,
        )


class ScoreRiskAsync(Task[OrderPlaced, RiskScoredOrder]):
    def execute(  # type: ignore[override]
        self,
        message: Message[OrderPlaced],
        **kwargs: object,
    ) -> Awaitable[RiskScoredOrder]:
        return self._execute(message, **kwargs)

    async def _execute(
        self,
        message: Message[OrderPlaced],
        **kwargs: object,
    ) -> RiskScoredOrder:
        client = kwargs["client"]
        if not isinstance(client, FakeRiskClient):
            raise TypeError("client must be FakeRiskClient.")
        risk_band = await client.risk_band(message.payload.amount)
        return RiskScoredOrder(order_id=message.payload.order_id, risk_band=risk_band)


class FakePricingClient:
    def __init__(self, events: ResourceEvents, client_id: int) -> None:
        self._events = events
        self.client_id = client_id

    def __enter__(self) -> FakePricingClient:
        self._events.opened.append(self.client_id)
        return self

    def __exit__(self, *args: object) -> None:
        self._events.closed.append(self.client_id)
        return None

    def price_band(self, amount: int) -> str:
        return "high" if amount >= 100 else "low"


class FakeRiskClient:
    async def __aenter__(self) -> FakeRiskClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def risk_band(self, amount: int) -> str:
        return "high" if amount >= 100 else "low"


@dataclass(frozen=True)
class StreamFlowCase:
    """Reusable public-DSL flow case for compiler and adapter tests."""

    flow: StreamFlow[Any, Any]
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

    def create_pricing_client(self) -> FakePricingClient:
        """Create a fake pricing client with a stable test identifier."""
        self._next_client_id += 1
        return FakePricingClient(self, self._next_client_id)


def build_simple_validation_flow_case() -> StreamFlowCase:
    """Build the smallest topic-in, task, topic-out StreamFlow case."""
    flow = StreamFlow(
        name="orders_validate",
        source=FromTopic("orders.raw", payload=OrderPlaced),
        process=Process(ValidateOrder()),
        output=IntoTopic("orders.validated", payload=ValidatedOrder),
    )
    message = Message(
        payload=OrderPlaced(order_id="o-1", amount=100),
        meta=MessageMeta(message_id="o-1"),
    )
    return StreamFlowCase(
        flow=flow,
        config=OmegaConf.create(_streaming_kafka_config()),
        input_messages=(message,),
        expected_payloads=(ValidatedOrder(order_id="o-1", accepted=True),),
    )


def build_router_flow_case() -> StreamFlowCase:
    """Build a topic flow with a Router that has multiple branches."""
    flow = StreamFlow(
        name="orders_route",
        source=FromTopic("orders.raw", payload=OrderPlaced),
        process=Process(
            Router.by(
                msg.payload.segment,
                routes={
                    "vip": Process(MarkVipOrder()),
                    "standard": Process(MarkStandardOrder()),
                },
                default=Process(MarkManualOrder()),
            )
        ),
        output=IntoTopic("orders.routed", payload=RoutedOrder),
    )
    return StreamFlowCase(
        flow=flow,
        config=OmegaConf.create(_streaming_kafka_config()),
        input_messages=(
            _order_message("o-1", 100, "vip"),
            _order_message("o-2", 50, "standard"),
            _order_message("o-3", 500, "unknown"),
        ),
        expected_payloads=(
            RoutedOrder(order_id="o-1", lane="vip"),
            RoutedOrder(order_id="o-2", lane="standard"),
            RoutedOrder(order_id="o-3", lane="manual"),
        ),
    )


def build_with_batch_flow_case() -> StreamFlowCase:
    """Build a batch flow using With and ForEach, so the sink sees individual messages."""
    events = ResourceEvents()
    flow = StreamFlow(
        name="orders_price_batch",
        source=FromTopic("orders.raw", payload=OrderPlaced),
        process=Process(
            CollectBatch(max_records=2, timeout_ms=1000),
            With(
                task=PriceOrder(),
                client=events.create_pricing_client(),
                scope=ResourceScope.WORKER,
            ),
            ForEach(),
        ),
        output=IntoTopic("orders.priced", payload=PricedOrder),
    )
    return StreamFlowCase(
        flow=flow,
        config=OmegaConf.create(_streaming_kafka_config()),
        input_messages=(
            _order_message("o-1", 100, "vip"),
            _order_message("o-2", 50, "standard"),
            _order_message("o-3", 500, "vip"),
            _order_message("o-4", 20, "standard"),
        ),
        expected_payloads=(
            PricedOrder(order_id="o-1", price_band="high", client_id=1),
            PricedOrder(order_id="o-2", price_band="low", client_id=1),
            PricedOrder(order_id="o-3", price_band="high", client_id=1),
            PricedOrder(order_id="o-4", price_band="low", client_id=1),
        ),
        resource_events=events,
    )


def build_with_batch_scope_flow_case() -> StreamFlowCase:
    """Build a batch flow using a fresh ContextFactory client per batch."""
    events = ResourceEvents()
    flow = StreamFlow(
        name="orders_price_batch_scope",
        source=FromTopic("orders.raw", payload=OrderPlaced),
        process=Process(
            CollectBatch(max_records=2, timeout_ms=1000),
            With(
                task=PriceOrder(),
                client=ContextFactory(events.create_pricing_client),
                scope=ResourceScope.BATCH,
            ),
            ForEach(),
        ),
        output=IntoTopic("orders.priced.batch_scope", payload=PricedOrder),
    )
    return StreamFlowCase(
        flow=flow,
        config=OmegaConf.create(_streaming_kafka_config()),
        input_messages=(
            _order_message("o-1", 100, "vip"),
            _order_message("o-2", 50, "standard"),
            _order_message("o-3", 500, "vip"),
            _order_message("o-4", 20, "standard"),
        ),
        expected_payloads=(
            PricedOrder(order_id="o-1", price_band="high", client_id=1),
            PricedOrder(order_id="o-2", price_band="low", client_id=1),
            PricedOrder(order_id="o-3", price_band="high", client_id=2),
            PricedOrder(order_id="o-4", price_band="low", client_id=2),
        ),
        resource_events=events,
    )


def build_async_one_flow_case() -> StreamFlowCase:
    """Build a batch flow using WithAsync.one() to emit individual async results."""
    flow: StreamFlow[Any, Any] = StreamFlow(
        name="orders_score_async_each",
        source=FromTopic("orders.raw", payload=OrderPlaced),
        process=Process(
            CollectBatch(max_records=2, timeout_ms=1000),
            WithAsync(
                task=ScoreRiskAsync(),
                client=FakeRiskClient(),
                max_concurrency=2,
                scope=ResourceScope.WORKER,
            ).one(IntoTopic("orders.scored", payload=RiskScoredOrder)),
        ),
    )
    return StreamFlowCase(
        flow=flow,
        config=OmegaConf.create(_streaming_kafka_config()),
        input_messages=(
            _order_message("o-1", 100, "vip"),
            _order_message("o-2", 50, "standard"),
        ),
        expected_payloads=(
            RiskScoredOrder(order_id="o-1", risk_band="high"),
            RiskScoredOrder(order_id="o-2", risk_band="low"),
        ),
    )


def _order_message(order_id: str, amount: int, segment: str) -> Message[OrderPlaced]:
    return Message(
        payload=OrderPlaced(order_id=order_id, amount=amount, segment=segment),
        meta=MessageMeta(message_id=order_id),
    )


def _streaming_kafka_config() -> dict[str, Any]:
    return {
        "kafka": {
            "consumer": {
                "brokers": ["localhost:9092"],
                "group_id": "test",
                "topics": ["orders.raw"],
            },
            "producer": {"brokers": ["localhost:9092"], "topic": "orders.validated"},
            "producers": {
                "orders.validated": {
                    "brokers": ["localhost:9092"],
                    "topic": "orders.validated",
                },
                "orders.routed": {
                    "brokers": ["localhost:9092"],
                    "topic": "orders.routed",
                },
                "orders.priced": {
                    "brokers": ["localhost:9092"],
                    "topic": "orders.priced",
                },
                "orders.priced.batch_scope": {
                    "brokers": ["localhost:9092"],
                    "topic": "orders.priced.batch_scope",
                },
                "orders.scored": {
                    "brokers": ["localhost:9092"],
                    "topic": "orders.scored",
                },
            },
        }
    }
