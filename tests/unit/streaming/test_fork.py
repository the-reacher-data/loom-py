"""Tests for terminal Fork branching and branch-index wiring."""

from __future__ import annotations

from typing import Any

import pytest
from omegaconf import OmegaConf

from loom.core.model import LoomStruct
from loom.streaming import Fork, ForkRoute, FromTopic, IntoTopic, Message, Process, StreamFlow
from loom.streaming.compiler import compile_flow


class _Order(LoomStruct):
    channel: str
    order_id: str


class _BranchA(LoomStruct):
    order_id: str


class _BranchB(LoomStruct):
    order_id: str


class _BranchDefault(LoomStruct):
    order_id: str


class _HasChannelA:
    def matches(self, message: Message[_Order]) -> bool:
        return message.payload.channel == "a"


class _HasChannelB:
    def matches(self, message: Message[_Order]) -> bool:
        return message.payload.channel == "b"


class _ChannelSelector:
    def select(self, message: Message[_Order]) -> object:
        return message.payload.channel


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
                "orders.a": {"brokers": ["localhost:9092"], "topic": "orders.a"},
                "orders.b": {"brokers": ["localhost:9092"], "topic": "orders.b"},
                "orders.default": {"brokers": ["localhost:9092"], "topic": "orders.default"},
            },
        }
    }


def _fork_when_flow() -> StreamFlow[_Order, Any]:
    return StreamFlow(
        name="orders_fork_when",
        source=FromTopic("orders.raw", payload=_Order),
        process=Process(
            Fork.when(
                (
                    ForkRoute(
                        when=_HasChannelA(),
                        process=Process(IntoTopic("orders.a", payload=_BranchA)),
                    ),
                    ForkRoute(
                        when=_HasChannelB(),
                        process=Process(IntoTopic("orders.b", payload=_BranchB)),
                    ),
                ),
                default=Process(IntoTopic("orders.default", payload=_BranchDefault)),
            )
        ),
    )


def _fork_by_flow() -> StreamFlow[_Order, Any]:
    return StreamFlow(
        name="orders_fork_by",
        source=FromTopic("orders.raw", payload=_Order),
        process=Process(
            Fork.by(
                selector=_ChannelSelector(),
                branches={
                    "a": Process(IntoTopic("orders.a", payload=_BranchA)),
                    "b": Process(IntoTopic("orders.b", payload=_BranchB)),
                },
                default=Process(IntoTopic("orders.default", payload=_BranchDefault)),
            )
        ),
    )


class TestForkDSL:
    def test_fork_by_rejects_empty_branches(self) -> None:
        with pytest.raises(ValueError, match="Fork.by requires at least one keyed route"):
            Fork.by(_ChannelSelector(), {})

    def test_fork_when_rejects_empty_routes(self) -> None:
        with pytest.raises(ValueError, match="Fork.when requires at least one predicate route"):
            Fork.when(())

    def test_fork_by_marks_keyed_kind(self) -> None:
        node = Fork.by(_ChannelSelector(), {"a": Process(IntoTopic("orders.a", payload=_BranchA))})

        assert node.kind is not None
        assert node.kind.value == "keyed"

    def test_fork_when_marks_predicate_kind(self) -> None:
        node = Fork.when(
            (
                ForkRoute(
                    when=_HasChannelA(),
                    process=Process(IntoTopic("orders.a", payload=_BranchA)),
                ),
            )
        )

        assert node.kind.value == "predicate"


class TestForkCompiler:
    def test_fork_when_compiles_three_terminal_branches(self) -> None:
        plan = compile_flow(_fork_when_flow(), runtime_config=OmegaConf.create(_kafka_config()))

        topics = {sink.topic for sink in plan.terminal_sinks.values()}
        assert topics == {"orders.a", "orders.b", "orders.default"}

    def test_fork_by_compiles_three_terminal_branches(self) -> None:
        plan = compile_flow(_fork_by_flow(), runtime_config=OmegaConf.create(_kafka_config()))

        topics = {sink.topic for sink in plan.terminal_sinks.values()}
        assert topics == {"orders.a", "orders.b", "orders.default"}
