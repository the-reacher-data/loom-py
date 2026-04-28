"""Shared contract test models and helpers."""

from __future__ import annotations

from loom.core.model import LoomStruct
from loom.streaming import Message, RecordStep


class Order(LoomStruct):
    order_id: str


class ValidatedOrder(LoomStruct):
    order_id: str


class Client:
    """Test-only resource implementation for protocol checks."""


class ClientFactory:
    def create(self) -> Client:
        return Client()

    def close(self, resource: Client) -> None:
        self.closed = resource


class Context:
    def __init__(self, resource: Client) -> None:
        self._resource = resource

    @property
    def resource(self) -> Client:
        return self._resource


class ValidateOrder(RecordStep[Order, ValidatedOrder]):
    def execute(self, message: Message[Order], **kwargs: object) -> ValidatedOrder:
        return ValidatedOrder(order_id=message.payload.order_id)


class NamedValidateOrder(RecordStep[Order, ValidatedOrder]):
    name = "custom"

    def execute(self, message: Message[Order], **kwargs: object) -> ValidatedOrder:
        return ValidatedOrder(order_id=message.payload.order_id)
