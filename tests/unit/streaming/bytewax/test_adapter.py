"""TDD for Bytewax runtime adapter — end-to-end execution.

These tests define how a :class:`loom.streaming.StreamFlow` is translated
into an executable Bytewax :class:`Dataflow`.
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("bytewax")

from loom.core.model import LoomStruct
from loom.streaming.compiler._plan import (
    CompiledNode,
    CompiledPlan,
    CompiledSink,
    CompiledSource,
)
from loom.streaming.core._errors import ErrorKind
from loom.streaming.core._message import Message, MessageMeta
from loom.streaming.kafka import KafkaRecord, MessageDescriptor, MsgspecCodec, build_message
from loom.streaming.kafka._config import ConsumerSettings, ProducerSettings
from loom.streaming.nodes._boundary import IntoTopic
from loom.streaming.nodes._shape import Drain, StreamShape
from loom.streaming.nodes._task import Task
from loom.streaming.testing import StreamingTestRunner


class _Order(LoomStruct):
    order_id: str


class _Result(LoomStruct):
    value: str


class _DoubleTask(Task[_Order, _Result]):
    def execute(self, message: Message[_Order], **kwargs: object) -> _Result:
        return _Result(value=message.payload.order_id * 2)


class _SuffixTask(Task[_Result, _Result]):
    def execute(self, message: Message[_Result], **kwargs: object) -> _Result:
        return _Result(value=f"{message.payload.value}:ok")


def _dummy_consumer() -> ConsumerSettings:
    return ConsumerSettings(
        brokers=("localhost:9092",),
        group_id="test",
        topics=("in",),
    )


def _message(payload: LoomStruct) -> Message[LoomStruct]:
    return Message(payload=payload, meta=MessageMeta(message_id="msg-1"))


def _build_plan(
    *nodes: object,
    output: IntoTopic[Any] | None = None,
    error_routes: dict[ErrorKind, CompiledSink] | None = None,
) -> CompiledPlan:
    """Build a minimal CompiledPlan for testing."""
    compiled_nodes = [
        CompiledNode(node=node, input_shape=StreamShape.RECORD, output_shape=StreamShape.RECORD)
        for node in nodes
    ]
    compiled_output = None
    if output is not None:
        compiled_output = CompiledSink(
            settings=ProducerSettings(
                brokers=("localhost:9092",),
                client_id="test-producer",
                topic=output.name,
            ),
            topic=output.name,
            partition_policy=None,
        )
    return CompiledPlan(
        name="test_flow",
        source=CompiledSource(
            settings=_dummy_consumer(),
            topics=("in",),
            payload_type=_Order,
            shape=StreamShape.RECORD,
            decode_strategy="record",
        ),
        nodes=tuple(compiled_nodes),
        output=compiled_output,
        error_routes=error_routes or {},
        needs_async_bridge=False,
    )


class TestTaskNodeExecution:
    """A simple Task node must transform records end-to-end."""

    def test_single_task_produces_transformed_output(self) -> None:
        plan = _build_plan(_DoubleTask())
        runner = StreamingTestRunner(plan).with_messages([_message(_Order(order_id="A"))])
        runner.run()
        results = runner.output

        assert len(results) == 1
        assert isinstance(results[0], Message)
        assert results[0].payload == _Result(value="AA")
        assert results[0].meta.message_id == "msg-1"

    def test_task_chain_passes_message_to_next_task(self) -> None:
        plan = _build_plan(_DoubleTask(), _SuffixTask())
        runner = StreamingTestRunner(plan).with_messages([_message(_Order(order_id="A"))])
        runner.run()
        results = runner.output

        assert [message.payload.value for message in results] == ["AA:ok"]


class TestSourceDecode:
    """Raw Kafka records must be decoded before reaching DSL nodes."""

    def test_kafka_record_source_decodes_envelope_before_task_execution(self) -> None:
        plan = _build_plan(_DoubleTask())
        codec = MsgspecCodec[_Order]()
        envelope = build_message(
            _Order(order_id="A"),
            MessageDescriptor(message_type="order.created", message_version=1),
            correlation_id="corr-1",
            trace_id="trace-1",
            produced_at_ms=10,
        )
        source_record = KafkaRecord(
            topic="orders.in",
            key=b"tenant-a",
            value=codec.encode(envelope),
            headers={"h": b"1"},
            partition=0,
            offset=3,
            timestamp_ms=11,
        )
        runner = StreamingTestRunner(plan).with_messages([source_record])
        runner.run()
        results = runner.output

        assert len(results) == 1
        assert results[0].payload == _Result(value="AA")
        assert results[0].meta.message_id == "orders.in:0:3"
        assert results[0].meta.message_type == "order.created"
        assert results[0].meta.message_version == 1
        assert results[0].meta.correlation_id == "corr-1"
        assert results[0].meta.trace_id == "trace-1"
        assert results[0].meta.produced_at_ms == 10
        assert results[0].meta.key == b"tenant-a"
        assert results[0].meta.headers == {"h": b"1"}

    def test_wire_decode_error_routes_to_testing_error_sink(self) -> None:
        error_sink = CompiledSink(
            settings=ProducerSettings(
                brokers=("localhost:9092",),
                client_id="test-producer",
                topic="orders.dlq",
            ),
            topic="orders.dlq",
            partition_policy=None,
        )
        plan = _build_plan(_DoubleTask(), error_routes={ErrorKind.WIRE: error_sink})
        source_record = KafkaRecord(
            topic="orders.in",
            key=b"tenant-a",
            value=b"not-msgpack",
            headers={"h": b"1"},
            partition=0,
            offset=4,
            timestamp_ms=12,
        )
        runner = (
            StreamingTestRunner(plan).with_messages([source_record]).capture_errors(ErrorKind.WIRE)
        )
        runner.run()
        results = runner.output
        errors = runner.errors[ErrorKind.WIRE]

        assert results == []
        assert len(errors) == 1
        assert errors[0].error.kind is ErrorKind.WIRE
        assert errors[0].error.original_message is None
        assert errors[0].raw == b"not-msgpack"
        assert errors[0].topic == "orders.in"
        assert errors[0].partition == 0
        assert errors[0].offset == 4
        assert errors[0].key == b"tenant-a"
        assert errors[0].headers == {"h": b"1"}


class TestTerminalNodes:
    """Terminal DSL nodes must be executable with Bytewax testing sinks."""

    def test_into_topic_node_wires_terminal_sink(self) -> None:
        target = IntoTopic("out", payload=_Result)
        plan = _build_plan(_DoubleTask(), target)
        runner = StreamingTestRunner(plan).with_messages([_message(_Order(order_id="A"))])
        runner.run()
        results = runner.output

        assert [message.payload.value for message in results] == ["AA"]

    def test_drain_node_swallows_stream(self) -> None:
        plan = _build_plan(_DoubleTask(), Drain())
        runner = StreamingTestRunner(plan).with_messages([_message(_Order(order_id="A"))])
        runner.run()
        results = runner.output

        assert results == []

    def test_payload_helper_builds_default_test_metadata(self) -> None:
        plan = _build_plan(_DoubleTask())
        runner = StreamingTestRunner(plan).with_payloads([_Order(order_id="A")])

        runner.run()

        results = runner.output
        assert len(results) == 1
        assert results[0].meta.message_id == "test-0"
        assert results[0].meta.topic == "in"

    def test_into_topic_node_does_not_duplicate_flow_output(self) -> None:
        target = IntoTopic("out", payload=_Result)
        plan = _build_plan(_DoubleTask(), target, output=target)
        runner = StreamingTestRunner(plan).with_messages([_message(_Order(order_id="A"))])

        runner.run()

        results = runner.output
        assert [message.payload.value for message in results] == ["AA"]


class TestOutputAndErrorWiring:
    """Output and error routes must coexist without interfering."""

    def test_valid_output_and_wire_error_can_be_captured_together(self) -> None:
        error_sink = CompiledSink(
            settings=ProducerSettings(
                brokers=("localhost:9092",),
                client_id="test-producer",
                topic="orders.dlq",
            ),
            topic="orders.dlq",
            partition_policy=None,
        )
        target = IntoTopic("out", payload=_Result)
        codec = MsgspecCodec[_Order]()
        envelope = build_message(
            _Order(order_id="A"),
            MessageDescriptor(message_type="order.created", message_version=1),
        )
        valid_record = KafkaRecord(
            topic="orders.in",
            key=b"tenant-a",
            value=codec.encode(envelope),
            headers={},
            partition=0,
            offset=1,
            timestamp_ms=10,
        )
        invalid_record = KafkaRecord(
            topic="orders.in",
            key=b"tenant-a",
            value=b"bad-wire",
            headers={},
            partition=0,
            offset=2,
            timestamp_ms=11,
        )
        plan = _build_plan(_DoubleTask(), output=target, error_routes={ErrorKind.WIRE: error_sink})
        runner = (
            StreamingTestRunner(plan)
            .with_messages([valid_record, invalid_record])
            .capture_errors(ErrorKind.WIRE)
        )

        runner.run()

        assert [message.payload.value for message in runner.output] == ["AA"]
        errors = runner.errors[ErrorKind.WIRE]
        assert len(errors) == 1
        assert errors[0].raw == b"bad-wire"
        assert errors[0].offset == 2
