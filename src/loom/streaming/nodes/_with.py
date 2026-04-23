"""Scoped dependency declarations for streaming tasks."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from enum import StrEnum
from types import MappingProxyType
from typing import Generic, TypeGuard, TypeVar

from loom.core.config import Configurable
from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.streaming.nodes._boundary import IntoTopic
from loom.streaming.nodes._task import Task

InT = TypeVar("InT", bound=LoomStruct | LoomFrozenStruct)
OutT = TypeVar("OutT", bound=LoomStruct | LoomFrozenStruct)

SyncContextDependency = AbstractContextManager[object]
AsyncContextDependency = AbstractAsyncContextManager[object]
ContextDependency = SyncContextDependency | AsyncContextDependency


class ResourceScope(StrEnum):
    """Context-manager lifecycle for dependencies declared through ``With``."""

    WORKER = "worker"
    BATCH = "batch"


class ContextFactory(Configurable):
    """Factory that creates a fresh context manager on demand.

    Use this when you need a new context-manager instance per batch
    (``scope=BATCH``) or when you want to configure the factory from YAML.

    Args:
        factory: Callable that returns a context-manager instance.

    Example::

        db = ContextFactory(lambda: SessionLocal())
        With(task=ValidateOrder(), scope=BATCH, db=db)
    """

    __slots__ = ("_factory",)

    def __init__(self, factory: Callable[[], ContextDependency]) -> None:
        self._factory = factory

    def create(self) -> ContextDependency:
        """Return a fresh context-manager instance."""
        return self._factory()


class _WithBase(Generic[InT, OutT]):
    """Shared dependency scope declaration for streaming tasks.

    Args:
        task: Task declaration executed inside the dependency scope.
        scope: Lifecycle used when opening context-manager dependencies.
        **dependencies: Named dependencies injected into task execution.
    """

    __slots__ = (
        "_async_contexts",
        "_context_factories",
        "_plain_deps",
        "_sync_contexts",
        "scope",
        "task",
    )

    def __init__(
        self,
        task: Task[InT, OutT],
        scope: ResourceScope = ResourceScope.WORKER,
        **dependencies: object,
    ) -> None:
        self.task = task
        self.scope = scope
        sync_contexts: dict[str, SyncContextDependency] = {}
        async_contexts: dict[str, AsyncContextDependency] = {}
        context_factories: dict[str, ContextFactory] = {}
        plain_deps: dict[str, object] = {}
        for name, dependency in dependencies.items():
            if isinstance(dependency, ContextFactory):
                context_factories[name] = dependency
            elif _is_sync_context_manager(dependency):
                sync_contexts[name] = dependency
            elif _is_async_context_manager(dependency):
                async_contexts[name] = dependency
            else:
                plain_deps[name] = dependency
        self._sync_contexts = sync_contexts
        self._async_contexts = async_contexts
        self._context_factories = context_factories
        self._plain_deps = plain_deps

    @property
    def sync_contexts(self) -> Mapping[str, SyncContextDependency]:
        """Sync context-manager dependencies keyed by injection name."""
        return MappingProxyType(self._sync_contexts)

    @property
    def async_contexts(self) -> Mapping[str, AsyncContextDependency]:
        """Async context-manager dependencies keyed by injection name."""
        return MappingProxyType(self._async_contexts)

    @property
    def context_factories(self) -> Mapping[str, ContextFactory]:
        """Context-manager factories keyed by injection name."""
        return MappingProxyType(self._context_factories)

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


class With(_WithBase[InT, OutT]):
    """Declare a sync dependency scope around a streaming task.

    Args:
        task: Task declaration executed with injected dependencies.
        scope: Lifecycle used when opening context-manager dependencies.
        **dependencies: Named dependencies injected into task execution.

    Raises:
        TypeError: If an async context manager is passed directly.
    """

    __slots__ = ()

    def __init__(
        self,
        task: Task[InT, OutT],
        scope: ResourceScope = ResourceScope.WORKER,
        **dependencies: object,
    ) -> None:
        super().__init__(task, scope=scope, **dependencies)
        if self._async_contexts:
            names = ", ".join(self._async_contexts.keys())
            raise TypeError(
                f"With only accepts sync context managers. "
                f"Async context manager(s) found: {names}. "
                f"Use WithAsync for async context managers."
            )


class WithAsync(_WithBase[InT, OutT]):
    """Declare an async dependency scope around a streaming task.

    Args:
        task: Task declaration executed with injected dependencies.
        scope: Lifecycle used when opening context-manager dependencies.
        max_concurrency: Maximum concurrent executions requested by the
            compiler/runtime adapter.
        **dependencies: Named dependencies injected into task execution.

    Raises:
        TypeError: If a sync context manager is passed directly.
    """

    __slots__ = ("max_concurrency",)

    def __init__(
        self,
        task: Task[InT, OutT],
        scope: ResourceScope = ResourceScope.WORKER,
        max_concurrency: int = 10,
        **dependencies: object,
    ) -> None:
        if max_concurrency <= 0:
            raise ValueError("max_concurrency must be greater than zero")
        super().__init__(task, scope=scope, **dependencies)
        if self._sync_contexts:
            names = ", ".join(self._sync_contexts.keys())
            raise TypeError(
                f"WithAsync only accepts async context managers. "
                f"Sync context manager(s) found: {names}. "
                f"Use With for sync context managers."
            )
        self.max_concurrency = max_concurrency


class OneEmit(LoomFrozenStruct, Generic[InT, OutT], frozen=True):
    """Declaration returned by ``With.one`` and ``WithAsync.one``.

    Args:
        source: Scoped task declaration.
        into: Topic used to emit each result individually.
    """

    source: _WithBase[InT, OutT]
    into: IntoTopic[OutT]


def _is_sync_context_manager(value: object) -> TypeGuard[SyncContextDependency]:
    return isinstance(value, AbstractContextManager)


def _is_async_context_manager(value: object) -> TypeGuard[AsyncContextDependency]:
    return isinstance(value, AbstractAsyncContextManager)


__all__ = [
    "AsyncContextDependency",
    "ContextDependency",
    "ContextFactory",
    "OneEmit",
    "ResourceScope",
    "SyncContextDependency",
    "With",
    "WithAsync",
]
