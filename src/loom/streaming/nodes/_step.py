"""Streaming step declarations and resource contracts."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Any, ClassVar, Generic, Protocol, TypeVar, runtime_checkable

from loom.core.config import Configurable
from loom.streaming.core._message import Message, StreamPayload
from loom.streaming.nodes._shape import StreamShape

InT = TypeVar("InT", bound=StreamPayload, contravariant=True)
OutT = TypeVar("OutT", bound=StreamPayload, covariant=True)
ResourceT = TypeVar("ResourceT")
ResourceCoT = TypeVar("ResourceCoT", covariant=True)


@runtime_checkable
class ResourceFactory(Protocol[ResourceT]):
    """Create and close step resources under runtime control."""

    def create(self) -> ResourceT:
        """Create one worker-local resource."""
        ...

    def close(self, resource: ResourceT) -> None:
        """Close one resource created by this factory."""
        ...


@runtime_checkable
class StepContext(Protocol[ResourceCoT]):
    """Execution context with explicit resource access."""

    @property
    def resource(self) -> ResourceCoT:
        """Worker-local resource available to the step."""
        ...


class Step(Configurable, ABC, Generic[InT, OutT]):
    """Base class for declarative streaming steps."""

    resource: ClassVar[type[ResourceFactory[Any]] | None] = None
    name: ClassVar[str] = ""
    input_shape: ClassVar[StreamShape]
    output_shape: ClassVar[StreamShape]

    @classmethod
    def step_name(cls) -> str:
        """Resolved step name for observability and validation errors."""
        return cls.name or cls.__qualname__


class RecordStep(Step[InT, OutT], ABC):
    """Streaming step that consumes and produces one record at a time."""

    router_branch_safe: ClassVar[bool] = True
    input_shape: ClassVar[StreamShape] = StreamShape.RECORD
    output_shape: ClassVar[StreamShape] = StreamShape.RECORD

    @abstractmethod
    def execute(self, message: Message[InT], **kwargs: object) -> OutT:
        """Execute one record-oriented transformation."""


class BatchStep(Step[InT, OutT], ABC):
    """Streaming step that consumes and produces one batch at a time."""

    router_branch_safe: ClassVar[bool] = True
    input_shape: ClassVar[StreamShape] = StreamShape.BATCH
    output_shape: ClassVar[StreamShape] = StreamShape.BATCH

    @abstractmethod
    def execute(self, messages: list[Message[InT]], **kwargs: object) -> list[OutT]:
        """Execute one batch-oriented transformation."""


class ExpandStep(Step[InT, OutT], ABC):
    """Streaming step that expands one record into many output messages."""

    router_branch_safe: ClassVar[bool] = True
    input_shape: ClassVar[StreamShape] = StreamShape.RECORD
    output_shape: ClassVar[StreamShape] = StreamShape.RECORD

    @abstractmethod
    def execute(self, message: Message[InT], **kwargs: object) -> Iterable[Message[OutT]]:
        """Execute one record-oriented fan-out transformation."""


class BatchExpandStep(Step[InT, OutT], ABC):
    """Streaming step that expands one batch into many output messages."""

    router_branch_safe: ClassVar[bool] = True
    input_shape: ClassVar[StreamShape] = StreamShape.BATCH
    output_shape: ClassVar[StreamShape] = StreamShape.RECORD

    @abstractmethod
    def execute(
        self,
        messages: list[Message[InT]],
        **kwargs: object,
    ) -> Iterable[Message[OutT]]:
        """Execute one batch-oriented fan-out transformation."""


__all__ = [
    "BatchExpandStep",
    "BatchStep",
    "ExpandStep",
    "RecordStep",
    "ResourceFactory",
    "Step",
    "StepContext",
]
