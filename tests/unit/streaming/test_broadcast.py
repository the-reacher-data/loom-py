"""Tests for the Broadcast fan-out node — DSL, compiler, and adapter wiring."""

from __future__ import annotations

from typing import Any

import pytest
from omegaconf import OmegaConf

pytest.importorskip("bytewax")

from loom.core.model import LoomStruct
from loom.streaming import (
    Broadcast,
    BroadcastRoute,
    FromTopic,
    IntoTopic,
    Message,
    MessageMeta,
    Process,
    RecordStep,
    StreamFlow,
)
from loom.streaming.compiler import CompilationError, compile_flow
from loom.streaming.testing import StreamingTestRunner


class _Order(LoomStruct):
    order_id: str
    amount: int


class _AnalyticsEvent(LoomStruct):
    order_id: str


class _FulfillmentRequest(LoomStruct):
    order_id: str
    quantity: int


class _ToAnalytics(RecordStep[_Order, _AnalyticsEvent]):
    def execute(self, message: Message[_Order], **kwargs: object) -> _AnalyticsEvent:
        return _AnalyticsEvent(order_id=message.payload.order_id)


class _ToFulfillment(RecordStep[_Order, _FulfillmentRequest]):
    def execute(self, message: Message[_Order], **kwargs: object) -> _FulfillmentRequest:
        return _FulfillmentRequest(
            order_id=message.payload.order_id, quantity=message.payload.amount
        )


def _kafka_config() -> dict[str, Any]:
    return {
        "kafka": {
            "consumer": {
                "brokers": ["localhost:9092"],
                "group_id": "test",
                "topics": ["orders.raw"],
            },
            "producer": {"brokers": ["localhost:9092"], "topic": "fallback"},
            "producers": {
                "events.analytics": {"brokers": ["localhost:9092"], "topic": "events.analytics"},
                "orders.fulfillment": {
                    "brokers": ["localhost:9092"],
                    "topic": "orders.fulfillment",
                },
                "extra_output": {"brokers": ["localhost:9092"], "topic": "extra_output"},
            },
        }
    }


def _broadcast_flow() -> StreamFlow[_Order, Any]:
    return StreamFlow(
        name="orders_broadcast",
        source=FromTopic("orders.raw", payload=_Order),
        process=Process(
            Broadcast(
                BroadcastRoute(
                    process=Process(_ToAnalytics()),
                    output=IntoTopic("events.analytics", payload=_AnalyticsEvent),
                ),
                BroadcastRoute(
                    process=Process(_ToFulfillment()),
                    output=IntoTopic("orders.fulfillment", payload=_FulfillmentRequest),
                ),
            )
        ),
    )


class TestBroadcastDSL:
    def test_broadcast_stores_routes(self) -> None:
        route_a = BroadcastRoute(
            process=Process(_ToAnalytics()),
            output=IntoTopic("a", payload=_AnalyticsEvent),
        )
        route_b = BroadcastRoute(
            process=Process(_ToFulfillment()),
            output=IntoTopic("b", payload=_FulfillmentRequest),
        )
        node = Broadcast(route_a, route_b)

        assert node.routes == (route_a, route_b)

    def test_broadcast_rejects_empty_routes(self) -> None:
        with pytest.raises(ValueError, match="at least one route"):
            Broadcast()


class TestBroadcastCompiler:
    def test_broadcast_flow_compiles_successfully(self) -> None:
        plan = compile_flow(_broadcast_flow(), runtime_config=OmegaConf.create(_kafka_config()))

        assert plan.name == "orders_broadcast"
        assert plan.output is None

    def test_broadcast_compiles_terminal_sinks_per_branch(self) -> None:
        plan = compile_flow(_broadcast_flow(), runtime_config=OmegaConf.create(_kafka_config()))

        topics = {sink.topic for sink in plan.terminal_sinks.values()}
        assert "events.analytics" in topics
        assert "orders.fulfillment" in topics

    def test_broadcast_with_flow_output_raises_compilation_error(self) -> None:
        flow: StreamFlow[_Order, _Order] = StreamFlow(
            name="bad",
            source=FromTopic("orders.raw", payload=_Order),
            process=Process(
                Broadcast(
                    BroadcastRoute(
                        process=Process(_ToAnalytics()),
                        output=IntoTopic("events.analytics", payload=_AnalyticsEvent),
                    ),
                )
            ),
            output=IntoTopic("extra_output", payload=_Order),
        )

        with pytest.raises(CompilationError, match="Broadcast"):
            compile_flow(flow, runtime_config=OmegaConf.create(_kafka_config()))


class TestBroadcastAdapter:
    def test_broadcast_delivers_message_to_all_branches(self) -> None:
        order = _Order(order_id="ORD-1", amount=3)
        message = Message(payload=order, meta=MessageMeta(message_id="m1"))

        runner = StreamingTestRunner.from_dict(_broadcast_flow(), config=_kafka_config())
        runner.with_messages([message]).run()

        output_types = {type(msg.payload) for msg in runner.output}
        assert _AnalyticsEvent in output_types
        assert _FulfillmentRequest in output_types

    def test_broadcast_produces_one_output_per_branch_per_message(self) -> None:
        messages = [
            Message(
                payload=_Order(order_id=f"ORD-{i}", amount=i), meta=MessageMeta(message_id=f"m{i}")
            )
            for i in range(3)
        ]

        runner = StreamingTestRunner.from_dict(_broadcast_flow(), config=_kafka_config())
        runner.with_messages(messages).run()

        analytics = [r for r in runner.output if isinstance(r.payload, _AnalyticsEvent)]
        fulfillment = [r for r in runner.output if isinstance(r.payload, _FulfillmentRequest)]
        assert len(analytics) == 3
        assert len(fulfillment) == 3
