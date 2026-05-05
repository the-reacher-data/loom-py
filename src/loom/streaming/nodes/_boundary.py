"""Topic-oriented streaming boundaries and partitioning contracts."""

from __future__ import annotations

from enum import StrEnum
from typing import ClassVar, Generic, Protocol, TypeVar

from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.core.routing import LogicalRef, as_logical_ref
from loom.streaming.core._message import Message
from loom.streaming.nodes._shape import StreamShape

PayloadT = TypeVar("PayloadT", bound=LoomStruct | LoomFrozenStruct, contravariant=True)
MultiPayloadT = TypeVar("MultiPayloadT", bound=LoomStruct | LoomFrozenStruct, covariant=True)


class FromTopic(LoomFrozenStruct, Generic[PayloadT], frozen=True):
    """Declare a topic-based input boundary.

    Pattern:
        Boundary node.

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


class FromMultiTypeTopic(LoomFrozenStruct, Generic[MultiPayloadT], frozen=True):
    """Declare a heterogeneous topic-based input boundary.

    Reads from a single Kafka topic that carries multiple payload types.
    The runtime dispatches each record to the correct decoder using
    ``MessageEnvelope.meta.descriptor.message_type`` (and for
    ``ErrorEnvelope`` variants, ``ErrorEnvelope.payload_type``).

    Each type in ``payloads`` uses ``__loom_message_type__`` when present and
    otherwise falls back to the fully qualified Python name
    ``f"{type.__module__}.{type.__qualname__}"`` so the compiler can build the
    dispatch table at compile time without requiring extra boilerplate.

    Args:
        name: Logical input reference.
        payloads: Tuple of two or more expected payload types.
        shape: Declared source shape.

    Raises:
        ValueError: When fewer than two payload types are declared.

    Example:
        .. code-block:: python

            source = FromMultiTypeTopic(
                "events.all",
                payloads=(OrderCreated, OrderCancelled),
            )
    """

    name: str
    payloads: tuple[type[LoomStruct | LoomFrozenStruct], ...]
    shape: StreamShape = StreamShape.RECORD

    def __post_init__(self) -> None:
        if len(self.payloads) < 2:
            raise ValueError(
                "FromMultiTypeTopic requires at least two payload types. "
                "Use FromTopic for a single-type source."
            )

    @property
    def logical_ref(self) -> LogicalRef:
        """Logical input reference."""
        return as_logical_ref(self.name)


class IntoTopic(LoomFrozenStruct, Generic[PayloadT], frozen=True):
    """Declare a topic-based output boundary.

    Pattern:
        Boundary node.

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

    Pattern:
        Boundary helper.

    Args:
        strategy: Partition-key strategy used for output records.
        guarantee: Declared affinity guarantee offered by the strategy.
        allow_repartition: Whether the flow may override the incoming key.
    """

    strategy: PartitionStrategy[PayloadT]
    guarantee: PartitionGuarantee
    allow_repartition: bool = False


__all__ = [
    "FromMultiTypeTopic",
    "FromTopic",
    "IntoTopic",
    "PartitionGuarantee",
    "PartitionPolicy",
    "PartitionStrategy",
]
