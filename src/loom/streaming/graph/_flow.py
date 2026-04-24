"""Process and StreamFlow definitions for the streaming DSL."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from types import MappingProxyType
from typing import Any, Generic, TypeAlias, TypeVar

from loom.core.config import ConfigBinding
from loom.streaming.core._errors import ErrorKind
from loom.streaming.core._message import StreamPayload
from loom.streaming.nodes._boundary import FromTopic, IntoTopic
from loom.streaming.nodes._router import Router
from loom.streaming.nodes._shape import CollectBatch, Drain, ForEach
from loom.streaming.nodes._step import BatchExpandStep, BatchStep, ExpandStep, RecordStep
from loom.streaming.nodes._with import OneEmit, With, WithAsync

ProcessNode: TypeAlias = (
    ConfigBinding
    | RecordStep[Any, Any]
    | BatchStep[Any, Any]
    | ExpandStep[Any, Any]
    | BatchExpandStep[Any, Any]
    | With[Any, Any]
    | WithAsync[Any, Any]
    | OneEmit[Any, Any]
    | CollectBatch
    | ForEach
    | Drain
    | IntoTopic[Any]
    | Router[Any, Any]
)

InT = TypeVar("InT", bound=StreamPayload, contravariant=True)
OutT = TypeVar("OutT", bound=StreamPayload, covariant=True)


class Process(Generic[InT, OutT]):
    """Ordered collection of streaming nodes.

    Args:
        *nodes: One or more nodes to execute in order.
    """

    __slots__ = ("_nodes",)

    def __init__(self, *nodes: ProcessNode) -> None:
        if not nodes:
            raise ValueError("Process must contain at least one node.")
        self._nodes = nodes

    def __iter__(self) -> Iterator[ProcessNode]:
        return iter(self._nodes)

    def __len__(self) -> int:
        return len(self._nodes)

    @property
    def nodes(self) -> tuple[ProcessNode, ...]:
        """Immutable sequence of process nodes."""
        return self._nodes


class StreamFlow(Generic[InT, OutT]):
    """Complete streaming flow declaration.

    Args:
        name: Unique flow identifier.
        source: Input topic declaration.
        process: Ordered sequence of transformation nodes.
        output: Optional terminal output topic.
        errors: Optional error-route mappings.
    """

    __slots__ = ("_errors", "_name", "_output", "_process", "_source")

    def __init__(
        self,
        name: str,
        source: FromTopic[InT],
        process: Process[InT, OutT],
        output: IntoTopic[OutT] | None = None,
        errors: Mapping[ErrorKind, IntoTopic[StreamPayload]] | None = None,
    ) -> None:
        if not name:
            raise ValueError("StreamFlow.name must not be empty.")
        self._name = name
        self._source = source
        self._process = process
        self._output = output
        self._errors = dict(errors or {})

    @property
    def errors(self) -> Mapping[ErrorKind, IntoTopic[StreamPayload]]:
        """Explicit error routes keyed by error kind."""
        return MappingProxyType(self._errors)

    @property
    def name(self) -> str:
        """Flow identifier."""
        return self._name

    @property
    def output(self) -> IntoTopic[OutT] | None:
        """Terminal output topic, if any."""
        return self._output

    @property
    def process(self) -> Process[InT, OutT]:
        """Ordered node sequence."""
        return self._process

    @property
    def source(self) -> FromTopic[InT]:
        """Input topic declaration."""
        return self._source
