"""Shared Bytewax test cases and payload models."""

from __future__ import annotations

from loom.core.model import LoomFrozenStruct
from loom.streaming.core._message import Message, MessageMeta
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
