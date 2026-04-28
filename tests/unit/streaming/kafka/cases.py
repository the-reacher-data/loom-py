"""Shared Kafka test payload models."""

from __future__ import annotations

from loom.core.model import LoomFrozenStruct


class OrderCreated(LoomFrozenStruct, frozen=True):
    """Kafka payload used across message, wire, and client tests."""

    order_id: str
    amount: int


class ProductEvent(LoomFrozenStruct, frozen=True):
    """Kafka payload used for codec round-trip tests."""

    sku: str
    stock: int
