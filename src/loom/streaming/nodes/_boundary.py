"""Topic-oriented streaming boundaries and partitioning contracts."""

from __future__ import annotations

from enum import StrEnum
from typing import ClassVar, Generic, Protocol, TypeVar

from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.core.routing import LogicalRef, as_logical_ref
from loom.streaming.core._message import Message
from loom.streaming.nodes._shape import StreamShape

PayloadT = TypeVar("PayloadT", bound=LoomStruct | LoomFrozenStruct, contravariant=True)


class FromTopic(LoomFrozenStruct, Generic[PayloadT], frozen=True):
    """Declare a topic-based input boundary.

    Args:
        name: Logical input reference. By default this is also used as the
            Kafka config reference.
        payload: Logical payload type descriptor.
        shape: Declared source shape.
    """

    name: str
    payload: type[PayloadT]
    shape: StreamShape = StreamShape.RECORD

    @property
    def logical_ref(self) -> LogicalRef:
        """Logical input reference."""
        return as_logical_ref(self.name)


class IntoTopic(LoomFrozenStruct, Generic[PayloadT], frozen=True):
    """Declare a topic-based output boundary.

    Args:
        name: Logical output reference. By default this is also used as the
            Kafka config reference.
        payload: Optional logical payload type descriptor.
        shape: Declared output shape.
        partitioning: Optional partitioning policy.
        dlq: Optional dead-letter topic name.  When set, messages that fail
            delivery are routed to this topic instead of crashing the worker.
            The DLQ producer reuses the same broker settings as the main
            output and adds an ``x-dlq-error`` header with the failure reason.
    """

    name: str
    payload: type[PayloadT] | None = None
    shape: StreamShape = StreamShape.RECORD
    partitioning: PartitionPolicy[PayloadT] | None = None
    dlq: str | None = None
    router_branch_safe: ClassVar[bool] = True

    @property
    def logical_ref(self) -> LogicalRef:
        """Logical output reference."""
        return as_logical_ref(self.name)


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


__all__ = [
    "FromTopic",
    "IntoTopic",
    "PartitionGuarantee",
    "PartitionPolicy",
    "PartitionStrategy",
]
