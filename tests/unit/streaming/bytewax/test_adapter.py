"""TDD for Bytewax runtime adapter — end-to-end execution.

These tests define how a :class:`loom.streaming.StreamFlow` is translated
into an executable Bytewax :class:`Dataflow`.
"""

from __future__ import annotations

from typing import Any

from bytewax.testing import TestingSink, TestingSource, run_main

from loom.core.model import LoomStruct
from loom.streaming.bytewax._adapter import build_dataflow
from loom.streaming.compiler._plan import (
    CompiledNode,
    CompiledPlan,
    CompiledSink,
    CompiledSource,
)
from loom.streaming.core._message import Message, MessageMeta
from loom.streaming.kafka import KafkaRecord, MessageDescriptor, MsgspecCodec, build_message
from loom.streaming.kafka._config import ConsumerSettings, ProducerSettings
from loom.streaming.nodes._boundary import IntoTopic
from loom.streaming.nodes._shape import Drain, StreamShape
from loom.streaming.nodes._task import Task


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


def _build_plan(*nodes: object, output: IntoTopic[Any] | None = None) -> CompiledPlan:
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
        error_routes={},
        needs_async_bridge=False,
    )


class TestTaskNodeExecution:
    """A simple Task node must transform records end-to-end."""

    def test_single_task_produces_transformed_output(self) -> None:
        plan = _build_plan(_DoubleTask())
        source_data = [_message(_Order(order_id="A"))]
        results: list[Message[_Result]] = []

        flow = build_dataflow(
            plan,
            source=TestingSource(source_data),
            sink=TestingSink(results),
        )

        run_main(flow)  # type: ignore[no-untyped-call]

        assert len(results) == 1
        assert isinstance(results[0], Message)
        assert results[0].payload == _Result(value="AA")
        assert results[0].meta.message_id == "msg-1"

    def test_task_chain_passes_message_to_next_task(self) -> None:
        plan = _build_plan(_DoubleTask(), _SuffixTask())
        results: list[Message[_Result]] = []

        flow = build_dataflow(
            plan,
            source=TestingSource([_message(_Order(order_id="A"))]),
            sink=TestingSink(results),
        )

        run_main(flow)  # type: ignore[no-untyped-call]

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
        results: list[Message[_Result]] = []

        flow = build_dataflow(
            plan,
            source=TestingSource([source_record]),
            sink=TestingSink(results),
        )

        run_main(flow)  # type: ignore[no-untyped-call]

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


class TestTerminalNodes:
    """Terminal DSL nodes must be executable with Bytewax testing sinks."""

    def test_into_topic_node_wires_terminal_sink(self) -> None:
        target = IntoTopic("out", payload=_Result)
        plan = _build_plan(_DoubleTask(), target)
        results: list[Message[_Result]] = []

        flow = build_dataflow(
            plan,
            source=TestingSource([_message(_Order(order_id="A"))]),
            sink=TestingSink(results),
        )

        run_main(flow)  # type: ignore[no-untyped-call]

        assert [message.payload.value for message in results] == ["AA"]

    def test_drain_node_swallows_stream(self) -> None:
        plan = _build_plan(_DoubleTask(), Drain())
        results: list[Message[_Result]] = []

        flow = build_dataflow(
            plan,
            source=TestingSource([_message(_Order(order_id="A"))]),
            sink=TestingSink(results),
        )

        run_main(flow)  # type: ignore[no-untyped-call]

        assert results == []
