"""Scoped dependency declarations for streaming tasks."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Generic, TypeVar

from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.streaming._boundary import IntoTopic
from loom.streaming._task import Task

InT = TypeVar("InT", bound=LoomStruct | LoomFrozenStruct)
OutT = TypeVar("OutT", bound=LoomStruct | LoomFrozenStruct)


class _WithBase(Generic[InT, OutT]):
    """Shared dependency scope declaration for streaming tasks.

    Args:
        task: Task declaration executed inside the dependency scope.
        **dependencies: Named dependencies injected into task execution.
    """

    __slots__ = ("_contexts", "_plain_deps", "task")

    def __init__(self, task: Task[InT, OutT], **dependencies: object) -> None:
        self.task = task
        contexts: dict[str, object] = {}
        plain_deps: dict[str, object] = {}
        for name, dependency in dependencies.items():
            if _is_context_manager(dependency):
                contexts[name] = dependency
            else:
                plain_deps[name] = dependency
        self._contexts = contexts
        self._plain_deps = plain_deps

    @property
    def contexts(self) -> Mapping[str, object]:
        """Context-manager dependencies keyed by injection name."""
        return MappingProxyType(self._contexts)

    @property
    def plain_deps(self) -> Mapping[str, object]:
        """Plain dependencies keyed by injection name."""
        return MappingProxyType(self._plain_deps)

    def one(self, into: IntoTopic[OutT]) -> OneEmit[InT, OutT]:
        """Declare individual result emission to a topic.

        Args:
            into: Output topic used for each produced item.

        Returns:
            Individual emission declaration for the scoped task.
        """
        return OneEmit(source=self, into=into)


class WithAsync(_WithBase[InT, OutT]):
    """Declare an async dependency scope around a streaming task.

    Args:
        task: Task declaration executed with injected dependencies.
        max_concurrency: Maximum concurrent executions requested by the
            compiler/runtime adapter.
        **dependencies: Named dependencies injected into task execution.
    """

    __slots__ = ("max_concurrency",)

    def __init__(
        self,
        task: Task[InT, OutT],
        max_concurrency: int = 10,
        **dependencies: object,
    ) -> None:
        if max_concurrency <= 0:
            raise ValueError("max_concurrency must be greater than zero")
        super().__init__(task, **dependencies)
        self.max_concurrency = max_concurrency


class With(_WithBase[InT, OutT]):
    """Declare a sync dependency scope around a streaming task.

    Args:
        task: Task declaration executed with injected dependencies.
        **dependencies: Named dependencies injected into task execution.
    """

    __slots__ = ()


class OneEmit(LoomFrozenStruct, Generic[InT, OutT], frozen=True):
    """Declaration returned by ``With.one`` and ``WithAsync.one``.

    Args:
        source: Scoped task declaration.
        into: Topic used to emit each result individually.
    """

    source: _WithBase[InT, OutT]
    into: IntoTopic[OutT]


def _is_context_manager(value: object) -> bool:
    return hasattr(value, "__aenter__") or hasattr(value, "__enter__")


__all__ = ["OneEmit", "With", "WithAsync"]
