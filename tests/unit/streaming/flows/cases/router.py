"""Router flow-case builder."""

from __future__ import annotations

from typing import Any

from omegaconf import DictConfig

from loom.streaming import FromTopic, IntoTopic, Process, Router, StreamFlow, msg
from tests.unit.streaming.flows.cases.shared import (
    _ORDERS_RAW_TOPIC,
    _ORDERS_ROUTED_TOPIC,
    MarkManualOrder,
    MarkStandardOrder,
    MarkVipOrder,
    OrderPlaced,
    RoutedOrder,
    StreamFlowCase,
    _build_case,
    _message,
)


def build_router_flow_case(config: DictConfig) -> StreamFlowCase:
    """Build a topic flow with a Router that has multiple branches."""
    flow: StreamFlow[Any, Any] = StreamFlow(
        name="orders_route",
        source=FromTopic(_ORDERS_RAW_TOPIC, payload=OrderPlaced),
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
        output=IntoTopic(_ORDERS_ROUTED_TOPIC, payload=RoutedOrder),
    )
    return _build_case(
        flow=flow,
        config=config,
        input_messages=(
            _message(OrderPlaced(order_id="o-1", amount=100, segment="vip")),
            _message(OrderPlaced(order_id="o-2", amount=50, segment="standard")),
            _message(OrderPlaced(order_id="o-3", amount=500, segment="unknown")),
        ),
        expected_payloads=(
            RoutedOrder(order_id="o-1", lane="vip"),
            RoutedOrder(order_id="o-2", lane="standard"),
            RoutedOrder(order_id="o-3", lane="manual"),
        ),
    )
