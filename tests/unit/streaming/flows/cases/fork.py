"""Fork flow-case builders."""

from __future__ import annotations

from typing import Any

from omegaconf import DictConfig

from loom.streaming import (
    CollectBatch,
    ContextFactory,
    Drain,
    Fork,
    ForkRoute,
    FromTopic,
    IntoTopic,
    Process,
    ResourceScope,
    StreamFlow,
    With,
    msg,
)
from tests.unit.streaming.flows.cases.shared import (
    _ORDERS_FORK_STANDARD_TOPIC,
    _ORDERS_FORK_VIP_TOPIC,
    OrderPlaced,
    PricedOrder,
    PriceOrder,
    ResourceEvents,
    RoutedOrder,
    StreamFlowCase,
    _build_case,
    _message,
    _routed_order_message,
)


def build_fork_flow_case(config: DictConfig) -> StreamFlowCase:
    """Build a terminal Fork flow with independent branch outputs."""
    flow: StreamFlow[Any, Any] = StreamFlow(
        name="orders_fork",
        source=FromTopic("orders.raw", payload=RoutedOrder),
        process=Process(
            Fork.by(
                msg.payload.lane,
                branches={
                    "vip": Process(IntoTopic(_ORDERS_FORK_VIP_TOPIC, payload=RoutedOrder)),
                    "standard": Process(
                        IntoTopic(_ORDERS_FORK_STANDARD_TOPIC, payload=RoutedOrder)
                    ),
                },
                default=Process(Drain()),
            )
        ),
    )
    return _build_case(
        flow=flow,
        config=config,
        input_messages=(
            _routed_order_message("o-1", "vip"),
            _routed_order_message("o-2", "standard"),
            _routed_order_message("o-3", "manual"),
        ),
        expected_payloads=(
            RoutedOrder(order_id="o-1", lane="vip"),
            RoutedOrder(order_id="o-2", lane="standard"),
        ),
    )


def build_fork_with_flow_case(config: DictConfig) -> StreamFlowCase:
    """Build a Fork flow whose matched branch opens a scoped resource."""
    events = ResourceEvents()
    flow: StreamFlow[Any, Any] = StreamFlow(
        name="orders_fork_with",
        source=FromTopic("orders.raw", payload=OrderPlaced),
        process=Process(
            Fork.when(
                routes=[
                    ForkRoute(
                        when=msg.payload.amount >= 100,
                        process=Process(
                            CollectBatch(max_records=1, timeout_ms=1000),
                            With(
                                process=Process(
                                    PriceOrder(),
                                    IntoTopic(_ORDERS_FORK_VIP_TOPIC, payload=PricedOrder),
                                ),
                                client=ContextFactory(events.create_pricing_client),
                                scope=ResourceScope.BATCH,
                            ),
                        ),
                    )
                ],
                default=Process(Drain()),
            )
        ),
    )
    return _build_case(
        flow=flow,
        config=config,
        input_messages=(
            _message(OrderPlaced(order_id="o-1", amount=100, segment="vip")),
            _message(OrderPlaced(order_id="o-2", amount=50, segment="standard")),
        ),
        expected_payloads=(PricedOrder(order_id="o-1", price_band="high", client_id=1),),
        resource_events=events,
    )


def build_fork_when_flow_case(config: DictConfig) -> StreamFlowCase:
    """Build a Fork.when flow with ordered predicate dispatch."""
    flow: StreamFlow[Any, Any] = StreamFlow(
        name="orders_fork_when",
        source=FromTopic("orders.raw", payload=OrderPlaced),
        process=Process(
            Fork.when(
                routes=[
                    ForkRoute(
                        when=msg.payload.segment == "vip",
                        process=Process(IntoTopic(_ORDERS_FORK_VIP_TOPIC, payload=OrderPlaced)),
                    ),
                    ForkRoute(
                        when=msg.payload.amount >= 100,
                        process=Process(
                            IntoTopic(_ORDERS_FORK_STANDARD_TOPIC, payload=OrderPlaced)
                        ),
                    ),
                ],
                default=Process(Drain()),
            )
        ),
    )
    return _build_case(
        flow=flow,
        config=config,
        input_messages=(
            _message(OrderPlaced(order_id="o-1", amount=100, segment="vip")),
            _message(OrderPlaced(order_id="o-2", amount=50, segment="standard")),
            _message(OrderPlaced(order_id="o-3", amount=500, segment="manual")),
        ),
        expected_payloads=(
            OrderPlaced(order_id="o-1", amount=100, segment="vip"),
            OrderPlaced(order_id="o-3", amount=500, segment="manual"),
        ),
    )
