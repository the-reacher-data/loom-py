"""Shared Bytewax test cases and payload models."""

from __future__ import annotations

from typing import Any

from loom.core.model import LoomFrozenStruct
from loom.streaming import IntoTopic
from loom.streaming.compiler._plan import (
    CompiledNode,
    CompiledPlan,
    CompiledSink,
    CompiledSource,
)
from loom.streaming.core._errors import ErrorKind
from loom.streaming.core._message import Message, MessageMeta
from loom.streaming.kafka._config import ConsumerSettings, ProducerSettings
from loom.streaming.nodes._shape import StreamShape
from loom.streaming.nodes._step import RecordStep


class Order(LoomFrozenStruct, frozen=True):
    """Canonical order payload used across Bytewax tests."""

    order_id: str


class Result(LoomFrozenStruct, frozen=True):
    """Canonical result payload used across Bytewax tests."""

    value: str


class DoubleStep(RecordStep[Order, Result]):
    """Duplicate an order id into the output payload."""

    def execute(self, message: Message[Order], **kwargs: object) -> Result:
        del kwargs
        return Result(value=message.payload.order_id * 2)


class SuffixStep(RecordStep[Result, Result]):
    """Append a suffix to the transformed payload."""

    def execute(self, message: Message[Result], **kwargs: object) -> Result:
        del kwargs
        return Result(value=f"{message.payload.value}:ok")


def build_message(payload: Order, *, message_id: str = "msg-1") -> Message[Order]:
    """Build a typed Bytewax message for tests."""

    return Message(payload=payload, meta=MessageMeta(message_id=message_id))


def build_compiled_plan(
    *nodes: object,
    output: IntoTopic[Any] | None = None,
    terminal_sinks: dict[tuple[int, ...], CompiledSink] | None = None,
    error_routes: dict[ErrorKind, CompiledSink] | None = None,
) -> CompiledPlan:
    """Build a reusable compiled plan for Bytewax adapter tests."""
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
            settings=ConsumerSettings(
                brokers=("localhost:9092",),
                group_id="test",
                topics=("in",),
            ),
            topics=("in",),
            payload_type=Order,
            shape=StreamShape.RECORD,
            decode_strategy="record",
        ),
        nodes=tuple(compiled_nodes),
        output=compiled_output,
        terminal_sinks=terminal_sinks or {},
        error_routes=error_routes or {},
        needs_async_bridge=False,
    )
