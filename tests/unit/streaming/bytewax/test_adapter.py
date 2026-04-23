"""TDD for Bytewax runtime adapter — end-to-end execution.

These tests define how a :class:`loom.streaming.StreamFlow` is translated
into an executable Bytewax :class:`Dataflow`.
"""

from __future__ import annotations

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


def _build_plan(*nodes: object, output: IntoTopic | None = None) -> CompiledPlan:
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

        run_main(flow)

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

        run_main(flow)

        assert [message.payload.value for message in results] == ["AA:ok"]


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

        run_main(flow)

        assert [message.payload.value for message in results] == ["AA"]

    def test_drain_node_swallows_stream(self) -> None:
        plan = _build_plan(_DoubleTask(), Drain())
        results: list[Message[_Result]] = []

        flow = build_dataflow(
            plan,
            source=TestingSource([_message(_Order(order_id="A"))]),
            sink=TestingSink(results),
        )

        run_main(flow)

        assert results == []
