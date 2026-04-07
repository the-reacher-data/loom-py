"""Generic write adapter: spec -> plan -> execute."""

from __future__ import annotations

from typing import Any, Generic

from loom.etl.storage.write.exec import FrameT, WriteExecutor
from loom.etl.storage.write.plan import TableWriteSpec, WritePlanner


class GenericTargetWriter(Generic[FrameT]):
    """Bridge that executes the canonical write flow.

    The flow is explicit and backend-agnostic:
    ``spec -> planner.plan(...) -> executor.execute(...)``.
    """

    def __init__(self, planner: WritePlanner, executor: WriteExecutor[FrameT]) -> None:
        self._planner = planner
        self._executor = executor

    def write(
        self,
        frame: FrameT,
        spec: TableWriteSpec,
        params_instance: Any,
        *,
        streaming: bool = False,
    ) -> None:
        """Plan and execute one write operation."""
        operation = self._planner.plan(spec, streaming=streaming)
        self._executor.execute(frame, operation, params_instance)
