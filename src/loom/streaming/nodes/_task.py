"""Streaming task declarations and resource contracts."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar, Generic, Protocol, TypeVar, runtime_checkable

from loom.core.config import Configurable
from loom.streaming.core._message import Message, StreamPayload

InT = TypeVar("InT", bound=StreamPayload, contravariant=True)
OutT = TypeVar("OutT", bound=StreamPayload, covariant=True)
ResourceT = TypeVar("ResourceT")
ResourceCoT = TypeVar("ResourceCoT", covariant=True)


@runtime_checkable
class ResourceFactory(Protocol[ResourceT]):
    """Create and close task resources under runtime control."""

    def create(self) -> ResourceT:
        """Create one worker-local resource."""
        ...

    def close(self, resource: ResourceT) -> None:
        """Close one resource created by this factory."""
        ...


@runtime_checkable
class TaskContext(Protocol[ResourceCoT]):
    """Execution context with explicit resource access."""

    @property
    def resource(self) -> ResourceCoT:
        """Worker-local resource available to the task."""
        ...


class Task(Configurable, ABC, Generic[InT, OutT]):
    """Base class for record-oriented streaming tasks.

    Subclass and implement :meth:`execute` with an explicit typed signature.
    """

    resource: ClassVar[type[ResourceFactory[Any]] | None] = None
    name: ClassVar[str] = ""

    @classmethod
    def task_name(cls) -> str:
        """Resolved task name for observability and validation errors."""
        return cls.name or cls.__qualname__

    @abstractmethod
    def execute(self, message: Message[InT], **kwargs: object) -> OutT:
        """Execute the task for one logical message.

        Extra keyword arguments are injected by the framework (e.g. opened
        context managers). The task declares what it needs via keyword-only
        parameters in concrete subclasses.
        """


class BatchTask(Configurable, ABC, Generic[InT, OutT]):
    """Base class for batch-oriented streaming tasks.

    Subclass and implement :meth:`execute` with an explicit typed signature.
    """

    resource: ClassVar[type[ResourceFactory[Any]] | None] = None
    name: ClassVar[str] = ""

    @classmethod
    def task_name(cls) -> str:
        """Resolved task name for observability and validation errors."""
        return cls.name or cls.__qualname__

    @abstractmethod
    def execute(self, messages: list[Message[InT]], **kwargs: object) -> list[OutT]:
        """Execute the task for one logical message batch.

        Extra keyword arguments are injected by the framework (e.g. opened
        context managers). The task declares what it needs via keyword-only
        parameters in concrete subclasses.
        """


__all__ = ["BatchTask", "ResourceFactory", "Task", "TaskContext"]
