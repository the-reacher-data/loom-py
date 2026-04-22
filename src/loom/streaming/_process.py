"""Streaming process and flow declarations."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Generic, TypeAlias, TypeVar

from loom.core.config import ConfigBinding
from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.streaming._boundary import FromTopic, IntoTopic
from loom.streaming._errors import ErrorKind

if TYPE_CHECKING:
    from loom.streaming._shape import CollectBatch, Drain, ForEach
    from loom.streaming._task import BatchTask, Task
    from loom.streaming._with import OneEmit, With, WithAsync
    from loom.streaming.routing import Router

InT = TypeVar("InT", bound=LoomStruct | LoomFrozenStruct)
OutT = TypeVar("OutT", bound=LoomStruct | LoomFrozenStruct)

if TYPE_CHECKING:
    ProcessNode: TypeAlias = (
        ConfigBinding
        | Task[Any, Any]
        | BatchTask[Any, Any]
        | With[Any, Any]
        | WithAsync[Any, Any]
        | OneEmit[Any, Any]
        | CollectBatch
        | ForEach
        | Drain
        | IntoTopic[Any]
        | Router[Any, Any]
    )
else:
    ProcessNode: TypeAlias = object


class Process(Generic[InT, OutT]):
    """Ordered streaming graph segment.

    Args:
        *nodes: Process nodes such as tasks, shape adapters, nested
            processes, routers, or terminal boundaries.

    Raises:
        ValueError: If no nodes are provided.
    """

    __slots__ = ("_nodes",)

    def __init__(self, *nodes: ProcessNode) -> None:
        if not nodes:
            raise ValueError("Process requires at least one node.")
        self._nodes = tuple(nodes)

    @property
    def nodes(self) -> tuple[ProcessNode, ...]:
        """Ordered process nodes."""
        return self._nodes

    def __iter__(self) -> Iterator[ProcessNode]:
        """Iterate over ordered process nodes."""
        return iter(self._nodes)

    def __len__(self) -> int:
        """Return the number of process nodes."""
        return len(self._nodes)


class StreamFlow(Generic[InT, OutT]):
    """Top-level streaming flow declaration.

    Args:
        name: Stable flow name.
        source: Input topic boundary.
        process: Main process graph.
        output: Optional fallback output boundary.
        errors: Optional explicit error routes.
    """

    __slots__ = ("name", "source", "process", "output", "_errors")

    def __init__(
        self,
        *,
        name: str,
        source: FromTopic[InT],
        process: Process[InT, OutT],
        output: IntoTopic[OutT] | None = None,
        errors: Mapping[ErrorKind, IntoTopic[Any]] | None = None,
    ) -> None:
        if not name:
            raise ValueError("StreamFlow.name must not be empty.")
        self.name = name
        self.source = source
        self.process = process
        self.output = output
        self._errors = dict(errors or {})

    @property
    def errors(self) -> Mapping[ErrorKind, IntoTopic[Any]]:
        """Explicit error routes keyed by error kind."""
        return MappingProxyType(self._errors)


__all__ = ["Process", "ProcessNode", "StreamFlow"]
