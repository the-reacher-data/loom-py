"""Bytewax execution tests for reusable public DSL flow examples."""

from __future__ import annotations

from typing import Any

import pytest
from omegaconf import OmegaConf

pytest.importorskip("bytewax")

from loom.core.model import LoomStruct
from loom.streaming import (
    ErrorEnvelope,
    ErrorKind,
    FromTopic,
    IntoTopic,
    Message,
    MessageMeta,
    Process,
    RecordStep,
    StreamFlow,
)
from loom.streaming.testing import StreamingTestRunner
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
            tuple(message.payload for message in results) == with_batch_flow_case.expected_payloads
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

    def test_runs_async_flow(
        self,
        async_flow_case: StreamFlowCase,
    ) -> None:
        results = _run_flow_case(async_flow_case)

        assert tuple(message.payload for message in results) == async_flow_case.expected_payloads

    def test_runs_fork_flow(
        self,
        fork_flow_case: StreamFlowCase,
    ) -> None:
        results = _run_flow_case(fork_flow_case)

        assert tuple(message.payload for message in results) == fork_flow_case.expected_payloads

    def test_runs_fork_when_flow(
        self,
        fork_when_flow_case: StreamFlowCase,
    ) -> None:
        results = _run_flow_case(fork_when_flow_case)

        assert (
            tuple(message.payload for message in results) == fork_when_flow_case.expected_payloads
        )

    def test_routes_record_step_errors_to_task_error_sink(self) -> None:
        flow: StreamFlow[Any, Any] = StreamFlow(
            name="orders_fail",
            source=FromTopic("orders.raw", payload=_Order),
            process=Process(_BoomStep(), IntoTopic("orders.validated", payload=_ValidatedOrder)),
            errors={ErrorKind.TASK: IntoTopic("orders.task.errors")},
        )
        runner = (
            StreamingTestRunner.from_flow(
                flow,
                runtime_config=OmegaConf.create(_kafka_config()),
            )
            .with_messages([_message(_Order(order_id="o-1", amount=10))])
            .capture_errors(ErrorKind.TASK)
        )

        runner.run()

        assert runner.output == []
        errors = runner.errors[ErrorKind.TASK]
        assert len(errors) == 1
        envelope = errors[0]
        assert isinstance(envelope, ErrorEnvelope)
        assert envelope.kind is ErrorKind.TASK
        assert envelope.reason == "boom"
        assert envelope.original_message is not None
        assert envelope.original_message.payload == _Order(order_id="o-1", amount=10)


def _run_flow_case(flow_case: StreamFlowCase) -> list[Message[Any]]:
    runner = StreamingTestRunner.from_flow(
        flow_case.flow,
        runtime_config=flow_case.config,
    ).with_messages(list(flow_case.input_messages))
    runner.run()
    return runner.output


class _Order(LoomStruct):
    order_id: str
    amount: int


class _ValidatedOrder(LoomStruct):
    order_id: str


class _BoomStep(RecordStep[_Order, _ValidatedOrder]):
    def execute(self, message: Message[_Order], **kwargs: object) -> _ValidatedOrder:
        del message, kwargs
        raise ValueError("boom")


def _message(payload: _Order) -> Message[_Order]:
    return Message(payload=payload, meta=MessageMeta(message_id=payload.order_id))


def _kafka_config() -> dict[str, object]:
    return {
        "kafka": {
            "consumer": {
                "brokers": ["localhost:9092"],
                "group_id": "test",
                "topics": ["orders.raw"],
            },
            "producer": {
                "brokers": ["localhost:9092"],
                "client_id": "test-producer",
            },
        }
    }
