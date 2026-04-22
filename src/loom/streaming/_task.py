"""Streaming task declarations."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar, Generic, TypeVar

from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.streaming._message import Message
from loom.streaming._resources import ResourceFactory

InT = TypeVar("InT", bound=LoomStruct | LoomFrozenStruct)
OutT = TypeVar("OutT", bound=LoomStruct | LoomFrozenStruct)


class Task(ABC, Generic[InT, OutT]):
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


class BatchTask(ABC, Generic[InT, OutT]):
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


__all__ = ["BatchTask", "Task"]
