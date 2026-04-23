"""Bytewax execution tests for reusable public DSL flow examples."""

from __future__ import annotations

from typing import Any

from bytewax.testing import TestingSink, TestingSource, run_main

from loom.streaming import Message, compile_flow
from loom.streaming.bytewax import build_dataflow
from tests.unit.streaming.support.flow_cases import StreamFlowCase


class TestBytewaxFlowExamples:
    """Bytewax execution coverage for shared public DSL flow examples."""

    def test_runs_simple_validation_flow(
        self,
        simple_validation_flow_case: StreamFlowCase,
    ) -> None:
        results = _run_flow_case(simple_validation_flow_case)

        assert tuple(message.payload for message in results) == (
            simple_validation_flow_case.expected_payloads
        )

    def test_runs_router_flow(
        self,
        router_flow_case: StreamFlowCase,
    ) -> None:
        results = _run_flow_case(router_flow_case)

        assert tuple(message.payload for message in results) == router_flow_case.expected_payloads

    def test_runs_with_batch_flow_with_for_each(
        self,
        with_batch_flow_case: StreamFlowCase,
    ) -> None:
        results = _run_flow_case(with_batch_flow_case)

        assert (
            tuple(message.payload for message in results)
            == with_batch_flow_case.expected_payloads
        )
        assert with_batch_flow_case.resource_events is not None
        assert with_batch_flow_case.resource_events.opened == [1]

    def test_runs_with_batch_scope_flow_with_for_each(
        self,
        with_batch_scope_flow_case: StreamFlowCase,
    ) -> None:
        results = _run_flow_case(with_batch_scope_flow_case)

        assert (
            tuple(message.payload for message in results)
            == with_batch_scope_flow_case.expected_payloads
        )
        assert with_batch_scope_flow_case.resource_events is not None
        assert with_batch_scope_flow_case.resource_events.opened == [1, 2]
        assert with_batch_scope_flow_case.resource_events.closed == [1, 2]

    def test_runs_async_one_flow(
        self,
        async_one_flow_case: StreamFlowCase,
    ) -> None:
        results = _run_flow_case(async_one_flow_case)

        assert (
            tuple(message.payload for message in results)
            == async_one_flow_case.expected_payloads
        )


def _run_flow_case(flow_case: StreamFlowCase) -> list[Message[Any]]:
    plan = compile_flow(flow_case.flow, runtime_config=flow_case.config)
    results: list[Message[Any]] = []
    dataflow = build_dataflow(
        plan,
        source=TestingSource(flow_case.input_messages),
        sink=TestingSink(results),
    )

    run_main(dataflow)  # type: ignore[no-untyped-call]
    return results
