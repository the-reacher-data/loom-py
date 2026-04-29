"""Simple flow-case builder."""

from __future__ import annotations

from typing import Any

from omegaconf import DictConfig

from loom.streaming import FromTopic, IntoTopic, Message, MessageMeta, Process, StreamFlow
from tests.unit.streaming.flows.cases.shared import (
    _ORDERS_RAW_TOPIC,
    _ORDERS_VALIDATED_TOPIC,
    OrderPlaced,
    StreamFlowCase,
    ValidatedOrder,
    ValidateOrder,
    _build_case,
)


def build_simple_validation_flow_case(config: DictConfig) -> StreamFlowCase:
    """Build the smallest topic-in, task, topic-out StreamFlow case."""
    flow: StreamFlow[Any, Any] = StreamFlow(
        name="orders_validate",
        source=FromTopic(_ORDERS_RAW_TOPIC, payload=OrderPlaced),
        process=Process(ValidateOrder()),
        output=IntoTopic(_ORDERS_VALIDATED_TOPIC, payload=ValidatedOrder),
    )
    message = Message(
        payload=OrderPlaced(order_id="o-1", amount=100),
        meta=MessageMeta(message_id="o-1"),
    )
    return _build_case(
        flow=flow,
        config=config,
        input_messages=(message,),
        expected_payloads=(ValidatedOrder(order_id="o-1", accepted=True),),
    )
