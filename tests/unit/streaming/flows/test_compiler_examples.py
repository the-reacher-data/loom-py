"""Compiler tests for reusable public DSL flow examples."""

from __future__ import annotations

import pytest

from loom.streaming import StreamShape, compile_flow
from loom.streaming.compiler._plan import CompiledPlan
from tests.unit.streaming.flows.cases import StreamFlowCase

pytestmark = pytest.mark.integration


class TestCompilerFlowExamples:
    """Compiler coverage for shared public DSL flow examples."""

    def test_compile_flow_examples(
        self,
        flow_case: StreamFlowCase,
    ) -> None:
        plan = compile_flow(flow_case.flow, runtime_config=flow_case.config)

        _assert_compiled_case(plan, flow_case)


def _assert_compiled_case(plan: CompiledPlan, case: StreamFlowCase) -> None:
    if case.flow.name == "orders_validate":
        compiled = plan
        assert compiled.name == "orders_validate"
        assert compiled.source.payload_type is not None
        assert compiled.source.shape is StreamShape.RECORD
        assert compiled.source.decode_strategy == "record"
        assert compiled.output is not None
        assert compiled.output.topic == "orders.validated"
        assert compiled.nodes[0].input_shape is StreamShape.RECORD
        assert compiled.nodes[0].output_shape is StreamShape.RECORD
        return

    if case.flow.name == "orders_route":
        compiled = plan
        assert compiled.name == "orders_route"
        assert compiled.output is not None
        assert compiled.output.topic == "orders.routed"
        assert compiled.nodes[0].input_shape is StreamShape.RECORD
        assert compiled.nodes[0].output_shape is StreamShape.RECORD
        return

    if case.flow.name in {"orders_price_batch", "orders_price_batch_scope"}:
        compiled = plan
        assert compiled.source.decode_strategy == "batch"
        assert compiled.output is None
        assert [node.output_shape for node in compiled.nodes] == [
            StreamShape.BATCH,
            StreamShape.NONE,
        ]
        return

    if case.flow.name == "orders_score_async_each":
        compiled = plan
        assert compiled.source.decode_strategy == "record"
        assert compiled.output is None
        assert compiled.needs_async_bridge is True
        assert [node.output_shape for node in compiled.nodes] == [StreamShape.NONE]
        return

    if case.flow.name == "orders_fork":
        compiled = plan
        assert compiled.output is None
        assert compiled.nodes[0].output_shape is StreamShape.NONE
        assert {sink.topic for sink in compiled.terminal_sinks.values()} == {
            "orders.fork.vip",
            "orders.fork.standard",
        }
        return

    if case.flow.name == "orders_fork_with":
        compiled = plan
        assert compiled.output is None
        assert {sink.topic for sink in compiled.terminal_sinks.values()} == {"orders.fork.vip"}
        return

    if case.flow.name == "orders_fork_when":
        compiled = plan
        assert compiled.output is None
        assert {sink.topic for sink in compiled.terminal_sinks.values()} == {
            "orders.fork.vip",
            "orders.fork.standard",
        }
        return

    raise AssertionError(f"Unhandled flow case {case.flow.name}")
