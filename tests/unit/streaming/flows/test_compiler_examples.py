"""Compiler tests for reusable public DSL flow examples."""

from __future__ import annotations

from loom.streaming import StreamShape, compile_flow
from tests.unit.streaming.flows.flow_cases import StreamFlowCase, ValidatedOrder


class TestCompilerFlowExamples:
    """Compiler coverage for shared public DSL flow examples."""

    def test_compile_simple_validation_flow(
        self,
        simple_validation_flow_case: StreamFlowCase,
    ) -> None:
        plan = compile_flow(
            simple_validation_flow_case.flow,
            runtime_config=simple_validation_flow_case.config,
        )

        assert plan.name == "orders_validate"
        assert plan.source.payload_type is not None
        assert plan.source.shape is StreamShape.RECORD
        assert plan.source.decode_strategy == "record"
        assert plan.output is not None
        assert plan.output.topic == "orders.validated"
        assert plan.nodes[0].input_shape is StreamShape.RECORD
        assert plan.nodes[0].output_shape is StreamShape.RECORD
        assert simple_validation_flow_case.expected_payloads == (
            ValidatedOrder(order_id="o-1", accepted=True),
        )

    def test_compile_router_flow(
        self,
        router_flow_case: StreamFlowCase,
    ) -> None:
        plan = compile_flow(
            router_flow_case.flow,
            runtime_config=router_flow_case.config,
        )

        assert plan.name == "orders_route"
        assert plan.output is not None
        assert plan.output.topic == "orders.routed"
        assert plan.nodes[0].input_shape is StreamShape.RECORD
        assert plan.nodes[0].output_shape is StreamShape.RECORD

    def test_compile_with_batch_flow_with_inner_terminal_output(
        self,
        with_batch_flow_case: StreamFlowCase,
    ) -> None:
        plan = compile_flow(
            with_batch_flow_case.flow,
            runtime_config=with_batch_flow_case.config,
        )

        assert plan.name == "orders_price_batch"
        assert plan.source.decode_strategy == "batch"
        assert plan.output is None
        assert [node.output_shape for node in plan.nodes] == [StreamShape.BATCH, StreamShape.NONE]

    def test_compile_with_batch_scope_flow_with_inner_terminal_output(
        self,
        with_batch_scope_flow_case: StreamFlowCase,
    ) -> None:
        plan = compile_flow(
            with_batch_scope_flow_case.flow,
            runtime_config=with_batch_scope_flow_case.config,
        )

        assert plan.name == "orders_price_batch_scope"
        assert plan.source.decode_strategy == "batch"
        assert plan.output is None
        assert [node.output_shape for node in plan.nodes] == [StreamShape.BATCH, StreamShape.NONE]

    def test_compile_async_flow_creates_async_bridge(
        self,
        async_flow_case: StreamFlowCase,
    ) -> None:
        plan = compile_flow(
            async_flow_case.flow,
            runtime_config=async_flow_case.config,
        )

        assert plan.name == "orders_score_async_each"
        assert plan.source.decode_strategy == "record"
        assert plan.output is None
        assert plan.needs_async_bridge is True
        assert [node.output_shape for node in plan.nodes] == [
            StreamShape.NONE,
        ]

    def test_compile_fork_flow_creates_branch_terminal_sinks(
        self,
        fork_flow_case: StreamFlowCase,
    ) -> None:
        plan = compile_flow(
            fork_flow_case.flow,
            runtime_config=fork_flow_case.config,
        )

        assert plan.name == "orders_fork"
        assert plan.output is None
        assert plan.nodes[0].output_shape is StreamShape.NONE
        assert {sink.topic for sink in plan.terminal_sinks.values()} == {
            "orders.fork.vip",
            "orders.fork.standard",
        }

    def test_compile_fork_flow_with_resources_creates_branch_terminal_sinks(
        self,
        fork_with_flow_case: StreamFlowCase,
    ) -> None:
        plan = compile_flow(
            fork_with_flow_case.flow,
            runtime_config=fork_with_flow_case.config,
        )

        assert plan.name == "orders_fork_with"
        assert plan.output is None
        assert {sink.topic for sink in plan.terminal_sinks.values()} == {"orders.fork.vip"}

    def test_compile_fork_when_flow_creates_branch_terminal_sinks(
        self,
        fork_when_flow_case: StreamFlowCase,
    ) -> None:
        plan = compile_flow(
            fork_when_flow_case.flow,
            runtime_config=fork_when_flow_case.config,
        )

        assert plan.name == "orders_fork_when"
        assert plan.output is None
        assert {sink.topic for sink in plan.terminal_sinks.values()} == {
            "orders.fork.vip",
            "orders.fork.standard",
        }
