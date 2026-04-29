"""Compiler tests for reusable public DSL flow examples."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from omegaconf import DictConfig

from loom.streaming import StreamShape, compile_flow
from loom.streaming.compiler._plan import CompiledPlan
from tests.unit.streaming.flows.cases import (
    StreamFlowCase,
    build_async_flow_case,
    build_fork_flow_case,
    build_fork_when_flow_case,
    build_fork_with_flow_case,
    build_router_flow_case,
    build_simple_validation_flow_case,
    build_with_batch_flow_case,
    build_with_batch_scope_flow_case,
)

pytestmark = pytest.mark.integration

CompilerCaseBuilder = Callable[[DictConfig], StreamFlowCase]


class TestCompilerFlowExamples:
    """Compiler coverage for shared public DSL flow examples."""

    @pytest.mark.parametrize(
        "case_builder",
        [
            build_simple_validation_flow_case,
            build_router_flow_case,
            build_with_batch_flow_case,
            build_with_batch_scope_flow_case,
            build_async_flow_case,
            build_fork_flow_case,
            build_fork_with_flow_case,
            build_fork_when_flow_case,
        ],
        ids=[
            "simple",
            "router",
            "with_batch",
            "with_batch_scope",
            "async",
            "fork",
            "fork_with",
            "fork_when",
        ],
    )
    def test_compile_flow_examples(
        self,
        case_builder: CompilerCaseBuilder,
        streaming_kafka_config: DictConfig,
    ) -> None:
        case = case_builder(streaming_kafka_config)
        plan = compile_flow(case.flow, runtime_config=case.config)

        _assert_compiled_case(plan, case)


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
