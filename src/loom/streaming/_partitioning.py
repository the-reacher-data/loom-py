"""Partitioning contracts for topic outputs."""

from __future__ import annotations

from enum import StrEnum
from typing import Generic, Protocol, TypeVar

from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.streaming._message import Message

PayloadT = TypeVar("PayloadT", bound=LoomStruct | LoomFrozenStruct)


class PartitionStrategy(Protocol[PayloadT]):
    """Compute the outgoing transport partition key for a message."""

    def partition_key(self, message: Message[PayloadT]) -> bytes | str | None:
        """Return the outgoing transport partition key."""


class PartitionGuarantee(StrEnum):
    """Declared affinity guarantee of a partitioning strategy."""

    NONE = "none"
    BEST_EFFORT = "best_effort"
    ENTITY_STABLE = "entity_stable"


class PartitionPolicy(LoomFrozenStruct, Generic[PayloadT], frozen=True):
    """Declarative partitioning policy for topic output.

    Args:
        strategy: Partition-key strategy used for output records.
        guarantee: Declared affinity guarantee offered by the strategy.
        allow_repartition: Whether the flow may override the incoming key.
    """

    strategy: PartitionStrategy[PayloadT]
    guarantee: PartitionGuarantee
    allow_repartition: bool = False


__all__ = ["PartitionGuarantee", "PartitionPolicy", "PartitionStrategy"]
