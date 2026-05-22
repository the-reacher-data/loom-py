"""MongoDB CDC input boundary."""

from __future__ import annotations

from typing import Any

import msgspec

from loom.core.model import LoomFrozenStruct
from loom.core.routing import LogicalRef, as_logical_ref
from loom.streaming.nodes._shape import StreamShape


class FromMongoCDC(LoomFrozenStruct, frozen=True):
    """Declare a MongoDB CDC stream as a streaming input boundary.

    Args:
        name: Logical input reference used to resolve Mongo source config.
        collections: Collection names to watch. An empty tuple means a
            database-level watch.
        watch_options: Driver watch kwargs resolved later by compiler/runtime.
        shape: Declared source shape.
    """

    name: str
    collections: tuple[str, ...] = ()
    watch_options: dict[str, Any] = msgspec.field(default_factory=dict)
    shape: StreamShape = StreamShape.RECORD

    @property
    def logical_ref(self) -> LogicalRef:
        """Return the logical input reference."""
        return as_logical_ref(self.name)


__all__ = ["FromMongoCDC"]
