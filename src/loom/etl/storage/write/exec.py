"""Write executor contract."""

from __future__ import annotations

from typing import Any, Protocol, TypeVar

from loom.etl.storage.write.ops import WriteOperation

FrameT = TypeVar("FrameT", contravariant=True)


class WriteExecutor(Protocol[FrameT]):
    """Execute a planned write operation for one backend frame type."""

    def execute(self, frame: FrameT, op: WriteOperation, params_instance: Any) -> None:
        """Execute one write operation."""
        ...
