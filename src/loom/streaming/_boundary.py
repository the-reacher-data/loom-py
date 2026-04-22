"""Topic-oriented streaming boundaries."""

from __future__ import annotations

from typing import Generic, TypeVar

from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.core.routing import LogicalRef, as_logical_ref
from loom.streaming._partitioning import PartitionPolicy
from loom.streaming._shape import StreamShape

PayloadT = TypeVar("PayloadT", bound=LoomStruct | LoomFrozenStruct)


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
    """

    name: str
    payload: type[PayloadT] | None = None
    shape: StreamShape = StreamShape.RECORD
    partitioning: PartitionPolicy[PayloadT] | None = None

    @property
    def logical_ref(self) -> LogicalRef:
        """Logical output reference."""
        return as_logical_ref(self.name)


__all__ = ["FromTopic", "IntoTopic"]
