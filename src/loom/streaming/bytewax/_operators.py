"""Resource lifecycle management for streaming Bytewax operators."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import (
    AbstractAsyncContextManager,
    AbstractContextManager,
    AsyncExitStack,
    ExitStack,
)
from typing import Any

from loom.core.async_bridge import AsyncBridge
from loom.streaming._with import ContextDependency, ResourceScope, With, WithAsync


class ResourceLifecycle:
    """Open and close scoped context-manager dependencies.

    Args:
        scope: Lifecycle declared by the DSL node.
        sync_contexts: Sync context managers keyed by injection name.
        async_contexts: Async context managers keyed by injection name.
        plain_deps: Plain dependencies keyed by injection name.
        bridge: Async bridge used for async context managers.
    """

    __slots__ = (
        "_async_contexts",
        "_async_stack",
        "_bridge",
        "_plain_deps",
        "_resources",
        "_scope",
        "_sync_contexts",
        "_sync_stack",
    )

    def __init__(
        self,
        *,
        scope: ResourceScope,
        sync_contexts: Mapping[str, AbstractContextManager[object]],
        async_contexts: Mapping[str, AbstractAsyncContextManager[object]],
        plain_deps: Mapping[str, object],
        bridge: AsyncBridge | None,
    ) -> None:
        if async_contexts and bridge is None:
            raise ValueError("Async context managers require an AsyncBridge.")
        self._scope = scope
        self._sync_contexts = dict(sync_contexts)
        self._async_contexts = dict(async_contexts)
        self._plain_deps = dict(plain_deps)
        self._bridge = bridge
        self._sync_stack: ExitStack | None = None
        self._async_stack: AsyncExitStack | None = None
        self._resources: dict[str, object] = {}

    def open_worker(self) -> dict[str, object]:
        """Open worker-scoped resources and return injectable dependencies."""
        if self._scope == ResourceScope.WORKER:
            self._resources = self._enter()
        self._resources.update(self._plain_deps)
        return dict(self._resources)

    def open_batch(self) -> dict[str, object]:
        """Open batch-scoped resources and return injectable dependencies."""
        if self._scope == ResourceScope.BATCH:
            self._resources = self._enter()
            self._resources.update(self._plain_deps)
        return dict(self._resources)

    def close_batch(self) -> None:
        """Close batch-scoped resources."""
        if self._scope == ResourceScope.BATCH:
            self._exit()

    def shutdown(self) -> None:
        """Close worker-scoped resources."""
        if self._scope == ResourceScope.WORKER:
            self._exit()

    def _enter(self) -> dict[str, object]:
        resources: dict[str, object] = {}
        if self._sync_contexts:
            self._sync_stack = ExitStack()
            resources.update(
                {
                    name: self._sync_stack.enter_context(context)
                    for name, context in self._sync_contexts.items()
                }
            )
        if self._async_contexts:
            resources.update(self._enter_async_contexts())
        return resources

    def _enter_async_contexts(self) -> dict[str, object]:
        if self._bridge is None:
            raise RuntimeError("Async context managers require an AsyncBridge.")
        self._async_stack = AsyncExitStack()

        async def _open_all() -> dict[str, object]:
            if self._async_stack is None:
                raise RuntimeError("AsyncExitStack was not initialized.")
            return {
                name: await self._async_stack.enter_async_context(context)
                for name, context in self._async_contexts.items()
            }

        return self._bridge.run(_open_all())

    def _exit(self) -> None:
        if self._async_stack is not None:
            if self._bridge is None:
                raise RuntimeError("Async context managers require an AsyncBridge.")
            self._bridge.run(self._async_stack.aclose())
            self._async_stack = None
        if self._sync_stack is not None:
            self._sync_stack.close()
            self._sync_stack = None
        self._resources = {}


def lifecycle_for(
    node: With[Any, Any] | WithAsync[Any, Any],
    *,
    bridge: AsyncBridge | None = None,
) -> ResourceLifecycle:
    """Create a resource lifecycle for a scoped dependency node.

    Args:
        node: DSL node carrying scope, contexts and plain dependencies.
        bridge: Async bridge for async context managers.

    Returns:
        Configured lifecycle ready for open/close calls.
    """
    sync_contexts, async_contexts = _split_contexts(node.contexts)
    return ResourceLifecycle(
        scope=node.scope,
        sync_contexts=sync_contexts,
        async_contexts=async_contexts,
        plain_deps=node.plain_deps,
        bridge=bridge,
    )


def _split_contexts(
    contexts: Mapping[str, ContextDependency],
) -> tuple[
    dict[str, AbstractContextManager[object]],
    dict[str, AbstractAsyncContextManager[object]],
]:
    sync_contexts: dict[str, AbstractContextManager[object]] = {}
    async_contexts: dict[str, AbstractAsyncContextManager[object]] = {}
    for name, context in contexts.items():
        if isinstance(context, AbstractAsyncContextManager):
            async_contexts[name] = context
        else:
            sync_contexts[name] = context
    return sync_contexts, async_contexts


resource_manager_for = lifecycle_for
ResourceManager = ResourceLifecycle

__all__ = ["ResourceLifecycle", "ResourceManager", "lifecycle_for", "resource_manager_for"]
