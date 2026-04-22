"""Pure operator functions for Bytewax adapter.

No Bytewax imports here — testable without the framework installed.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import AsyncExitStack, ExitStack
from typing import Any

from loom.core.async_bridge import AsyncWorker
from loom.streaming._with import ResourceScope, With, WithAsync


class _ResourceManager(ABC):
    """Lifecycle contract for context-manager dependencies."""

    @abstractmethod
    def open_worker(self) -> dict[str, Any]:
        """Called once at worker start."""
        ...

    @abstractmethod
    def open_batch(self) -> dict[str, Any]:
        """Called before each batch."""
        ...

    @abstractmethod
    def close_batch(self) -> None:
        """Called after each batch."""
        ...

    @abstractmethod
    def shutdown(self) -> None:
        """Called at worker shutdown."""
        ...


class _SyncResourceManager(_ResourceManager):
    """Manages sync context managers via :class:`contextlib.ExitStack`."""

    def __init__(self, node: With | WithAsync) -> None:
        self._scope = node.scope
        self._contexts = node.contexts
        self._plain_deps = node.plain_deps
        self._stack: ExitStack | None = None
        self._resources: dict[str, Any] = {}

    def open_worker(self) -> dict[str, Any]:
        if self._scope == ResourceScope.WORKER:
            self._resources = self._enter()
        self._resources.update(self._plain_deps)
        return self._resources

    def open_batch(self) -> dict[str, Any]:
        if self._scope == ResourceScope.BATCH:
            self._resources = self._enter()
            self._resources.update(self._plain_deps)
        return self._resources

    def close_batch(self) -> None:
        if self._scope == ResourceScope.BATCH:
            self._exit()

    def shutdown(self) -> None:
        if self._scope == ResourceScope.WORKER:
            self._exit()

    def _enter(self) -> dict[str, Any]:
        self._stack = ExitStack()
        return {name: self._stack.enter_context(cm) for name, cm in self._contexts.items()}

    def _exit(self) -> None:
        if self._stack is not None:
            self._stack.close()
            self._stack = None
            self._resources = {}


class _AsyncResourceManager(_ResourceManager):
    """Manages async context managers via :class:`contextlib.AsyncExitStack`."""

    def __init__(self, node: With | WithAsync, worker: AsyncWorker) -> None:
        self._scope = node.scope
        self._contexts = node.contexts
        self._plain_deps = node.plain_deps
        self._worker = worker
        self._stack: AsyncExitStack | None = None
        self._resources: dict[str, Any] = {}

    def open_worker(self) -> dict[str, Any]:
        if self._scope == ResourceScope.WORKER:
            self._resources = self._enter()
        self._resources.update(self._plain_deps)
        return self._resources

    def open_batch(self) -> dict[str, Any]:
        if self._scope == ResourceScope.BATCH:
            self._resources = self._enter()
            self._resources.update(self._plain_deps)
        return self._resources

    def close_batch(self) -> None:
        if self._scope == ResourceScope.BATCH:
            self._exit()

    def shutdown(self) -> None:
        if self._scope == ResourceScope.WORKER:
            self._exit()

    def _enter(self) -> dict[str, Any]:
        self._stack = AsyncExitStack()
        return {
            name: self._worker.run(self._stack.enter_async_context(cm))
            for name, cm in self._contexts.items()
        }

    def _exit(self) -> None:
        if self._stack is not None:
            self._worker.run(self._stack.aclose())
            self._stack = None
            self._resources = {}


def resource_manager_for(
    node: With | WithAsync, *, worker: AsyncWorker | None = None
) -> _ResourceManager:
    """Factory: returns the appropriate resource-manager implementation."""
    if worker is not None:
        return _AsyncResourceManager(node, worker)
    return _SyncResourceManager(node)
