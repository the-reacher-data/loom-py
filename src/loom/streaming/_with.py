"""Context-manager batch processing adapters for the streaming DSL."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any
from typing import Any as TypingAny

from loom.streaming._boundary import IntoTopic
from loom.streaming._task import Task


class ResourceScope(StrEnum):
    """Controls when context managers are opened/closed."""

    WORKER = "worker"  # once at worker start, exit at shutdown
    BATCH = "batch"  # enter before each batch, exit after each batch


class _WithBase:
    """Base for With / WithAsync: detects context managers among kwargs."""

    def __init__(
        self,
        task: Task[Any, Any],
        scope: ResourceScope = ResourceScope.WORKER,
        **dependencies: Any,
    ) -> None:
        self.task = task
        self.scope = scope
        self.contexts: dict[str, Any] = {}
        self.plain_deps: dict[str, Any] = {}
        for name, obj in dependencies.items():
            if hasattr(obj, "__aenter__") or hasattr(obj, "__enter__"):
                self.contexts[name] = obj
            else:
                self.plain_deps[name] = obj

    def one(self, into: IntoTopic[TypingAny]) -> OneEmit:
        """Fire-and-forget: emit each result individually to the given topic."""
        return OneEmit(self, into)


class WithAsync(_WithBase):
    """Async context-manager batch processor.

    Opens async context managers, executes the task concurrently over a batch
    via ``asyncio.gather``, and returns the full list of results.

    Use :meth:`one` to emit results individually instead.
    """

    def __init__(
        self,
        task: Task[Any, Any],
        scope: ResourceScope = ResourceScope.WORKER,
        max_concurrency: int = 10,
        **dependencies: Any,
    ) -> None:
        super().__init__(task, scope=scope, **dependencies)
        self.max_concurrency = max_concurrency


class With(_WithBase):
    """Sync context-manager batch processor.

    Opens sync context managers and executes the task over a batch.

    Use :meth:`one` to emit results individually instead.
    """


@dataclass(frozen=True)
class OneEmit:
    """Marker returned by ``.one(into)``: indicates individual emission mode."""

    source: _WithBase
    into: IntoTopic[TypingAny]
