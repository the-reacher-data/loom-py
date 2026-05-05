"""Boundary contract tests for streaming."""

from __future__ import annotations

from typing import Any

from loom.core.routing import LogicalRef
from loom.streaming import (
    FromTopic,
    IntoTopic,
    Message,
    PartitionGuarantee,
    PartitionPolicy,
    PartitionStrategy,
    StreamShape,
)
from tests.unit.streaming.contracts.cases import Order


class _OrderPartitionStrategy:
    def partition_key(self, message: Message[Order]) -> bytes | str | None:
        return message.payload.order_id


class TestBoundaryContracts:
    def test_topic_boundaries_hold_payload_classes_and_explicit_shapes(self) -> None:
        source = FromTopic("orders.in", payload=Order, shape=StreamShape.BATCH)
        target = IntoTopic("orders.out", payload=Order, shape=StreamShape.MANY)

        assert source.name == "orders.in"
        assert source.payload is Order
        assert source.shape is StreamShape.BATCH
        assert target.payload is Order
        assert target.shape is StreamShape.MANY

    def test_topic_boundaries_use_logical_refs_only(self) -> None:
        source = FromTopic("orders-input", payload=Order)
        target = IntoTopic("validated-orders", payload=Order)

        assert source.logical_ref == LogicalRef("orders-input")
        assert target.logical_ref == LogicalRef("validated-orders")

    def test_into_topic_can_be_used_without_payload_for_error_routes(self) -> None:
        target: IntoTopic[Any] = IntoTopic("orders.dlq")

        assert target.payload is None
        assert target.shape is StreamShape.RECORD

    def test_into_topic_can_declare_partitioning_policy(self) -> None:
        strategy: PartitionStrategy[Order] = _OrderPartitionStrategy()
        policy = PartitionPolicy(
            strategy=strategy,
            guarantee=PartitionGuarantee.ENTITY_STABLE,
            allow_repartition=True,
        )
        target = IntoTopic("orders.out", payload=Order, partitioning=policy)

        assert target.partitioning is policy
        assert policy.guarantee is PartitionGuarantee.ENTITY_STABLE
        assert policy.allow_repartition is True
