"""Shared compiler test models and config helpers."""

from __future__ import annotations

from loom.core.model import LoomStruct
from loom.streaming import RecordStep
from loom.streaming.core._message import Message


class Order(LoomStruct):
    order_id: str


class Result(LoomStruct):
    value: str


class FakeStep(RecordStep[Order, Result]):
    def execute(self, message: Message[Order], **kwargs: object) -> Result:
        return Result(value=message.payload.order_id)
