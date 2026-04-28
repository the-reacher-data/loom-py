"""Tests for the Broadcast fan-out node — DSL, compiler, and adapter wiring."""

from __future__ import annotations

from typing import Any

import pytest
from omegaconf import DictConfig

from loom.streaming import Broadcast
from loom.streaming.compiler import CompilationError, compile_flow
from loom.streaming.testing import StreamingTestRunner


class TestBroadcast:
    def test_broadcast_rejects_empty_routes(self) -> None:
        with pytest.raises(ValueError, match="at least one route"):
            Broadcast()

    def test_broadcast_flow_compiles_and_wires_branch_terminals(
        self,
        broadcast_flow: Any,
        streaming_kafka_config: DictConfig,
    ) -> None:
        plan = compile_flow(broadcast_flow, runtime_config=streaming_kafka_config)

        assert plan.name == "orders_broadcast"
        assert plan.output is None
        topics = {sink.topic for sink in plan.terminal_sinks.values()}
        assert "events.analytics" in topics
        assert "orders.fulfillment" in topics

    def test_broadcast_with_flow_output_raises_compilation_error(
        self,
        broadcast_bad_flow: Any,
        streaming_kafka_config: DictConfig,
    ) -> None:
        with pytest.raises(CompilationError, match="Broadcast"):
            compile_flow(broadcast_bad_flow, runtime_config=streaming_kafka_config)

    def test_broadcast_delivers_one_output_per_branch_per_message(
        self,
        broadcast_flow: Any,
        streaming_kafka_config: DictConfig,
        streaming_kafka_config_dict: dict[str, object],
        broadcast_messages: list[Any],
        broadcast_output_types: tuple[type[Any], type[Any]],
    ) -> None:
        analytics_type, fulfillment_type = broadcast_output_types
        runner = StreamingTestRunner.from_dict(
            broadcast_flow,
            config=streaming_kafka_config_dict,
        )
        runner.with_messages(broadcast_messages).run()

        analytics = [r for r in runner.output if isinstance(r.payload, analytics_type)]
        fulfillment = [r for r in runner.output if isinstance(r.payload, fulfillment_type)]
        assert len(analytics) == 3
        assert len(fulfillment) == 3

    def test_broadcast_delivers_message_to_all_branches(
        self,
        broadcast_flow: Any,
        streaming_kafka_config: DictConfig,
        streaming_kafka_config_dict: dict[str, object],
        broadcast_message: Any,
        broadcast_output_types: tuple[type[Any], type[Any]],
    ) -> None:
        analytics_type, fulfillment_type = broadcast_output_types
        runner = StreamingTestRunner.from_dict(
            broadcast_flow,
            config=streaming_kafka_config_dict,
        )
        runner.with_messages([broadcast_message]).run()

        output_types = {type(msg.payload) for msg in runner.output}
        assert analytics_type in output_types
        assert fulfillment_type in output_types

    def test_broadcast_route_holds_process_and_output(
        self,
        broadcast_route_pair: tuple[Any, Any],
    ) -> None:
        route_a, route_b = broadcast_route_pair
        node: Broadcast[Any] = Broadcast(route_a, route_b)

        assert node.routes == (route_a, route_b)
