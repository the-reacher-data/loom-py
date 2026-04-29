"""Bytewax execution tests for reusable public DSL flow examples."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from omegaconf import DictConfig

from loom.core.model import LoomStruct
from loom.streaming import (
    BatchStep,
    ErrorEnvelope,
    ErrorKind,
    FromTopic,
    IntoTopic,
    Message,
    MessageMeta,
    Process,
    RecordStep,
    Route,
    Router,
    StreamFlow,
    WithAsync,
    msg,
)
from loom.streaming.testing import StreamingTestRunner
from tests.unit.streaming.flows.cases import StreamFlowCase

pytestmark = pytest.mark.integration


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

    def test_runs_with_batch_flow_with_inner_terminal_output(
        self,
        with_batch_flow_case: StreamFlowCase,
    ) -> None:
        results = _run_flow_case(with_batch_flow_case)

        assert (
            tuple(message.payload for message in results) == with_batch_flow_case.expected_payloads
        )
        assert with_batch_flow_case.resource_events is not None
        assert with_batch_flow_case.resource_events.opened == [1]

    def test_runs_with_batch_scope_flow_with_inner_terminal_output(
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

    def test_routes_record_step_errors_to_task_error_sink(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        flow: StreamFlow[Any, Any] = StreamFlow(
            name="orders_fail",
            source=FromTopic("orders.raw", payload=_Order),
            process=Process(_BoomStep(), IntoTopic("orders.validated", payload=_ValidatedOrder)),
            errors={ErrorKind.TASK: IntoTopic("orders.task.errors")},
        )
        runner = (
            StreamingTestRunner.from_flow(
                flow,
                runtime_config=streaming_kafka_config,
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

    def test_with_async_task_timeout_captures_timed_out_messages_as_errors(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        """task_timeout_ms cancels slow inner-process tasks and routes them to error envelopes."""
        flow: StreamFlow[_Order, _ValidatedOrder] = StreamFlow(
            name="timeout_flow",
            source=FromTopic("orders.raw", payload=_Order),
            process=Process(
                WithAsync(
                    process=Process(
                        _SlowAsyncStep(),
                        IntoTopic("orders.validated", payload=_ValidatedOrder),
                    ),
                    task_timeout_ms=50,
                ),
            ),
        )
        runner = StreamingTestRunner.from_flow(flow, runtime_config=streaming_kafka_config)
        runner.capture_errors(ErrorKind.TASK)
        msg = _message(_Order(order_id="ORD-1", amount=1))

        runner.with_messages([msg]).run()

        assert len(runner.output) == 0
        assert len(runner.errors[ErrorKind.TASK]) == 1
        envelope = runner.errors[ErrorKind.TASK][0]
        assert isinstance(envelope, ErrorEnvelope)
        assert envelope.kind == ErrorKind.TASK

    def test_router_executes_batch_step_branch_as_singleton_batch(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        """BatchStep inside a Router branch must execute as a singleton batch."""
        flow: StreamFlow[_Order, _ValidatedOrder] = StreamFlow(
            name="router_batch_flow",
            source=FromTopic("orders.raw", payload=_Order),
            process=Process(
                Router.when(
                    routes=[
                        Route(
                            when=msg.payload.order_id != "",
                            process=Process(_UpperBatchStep()),
                        )
                    ],
                )
            ),
            output=IntoTopic("orders.out", payload=_ValidatedOrder),
        )
        runner = StreamingTestRunner.from_flow(flow, runtime_config=streaming_kafka_config)
        runner.with_messages([_message(_Order(order_id="ord-1", amount=10))]).run()

        assert len(runner.output) == 1
        assert runner.output[0].payload == _ValidatedOrder(order_id="ORD-1")


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


class _SlowAsyncStep(RecordStep[_Order, _ValidatedOrder]):
    async def execute(self, message: Message[_Order], **kwargs: object) -> _ValidatedOrder:
        await asyncio.sleep(10)
        return _ValidatedOrder(order_id=message.payload.order_id)


# ---------------------------------------------------------------------------
# Router + BatchStep — Router must execute BatchStep as singleton batch
# ---------------------------------------------------------------------------


class _UpperBatchStep(BatchStep[_Order, _ValidatedOrder]):
    def execute(self, messages: list[Message[_Order]], **kwargs: object) -> list[_ValidatedOrder]:
        return [_ValidatedOrder(order_id=m.payload.order_id.upper()) for m in messages]
