from __future__ import annotations

import inspect
from collections.abc import Callable
from contextvars import ContextVar
from typing import Any

# IMPORTANT — default=None, not default=[].  Flush and clear must also reset
# to None, never to [].  The invariant is: None ↔ "no active queue";
# a list ↔ "active queue for this context".
# A mutable list as ContextVar default would be shared across all contexts that
# have not called .set(), causing cross-request contamination.  Resetting to []
# (instead of None) has the same effect: asyncio tasks copy the context shallowly,
# so all siblings would share the same list object.  Resetting to None ensures
# each task creates its own list on first access via _get_or_init().
_pending: ContextVar[list[Callable[[], Any]] | None] = ContextVar(
    "_loom_pending_dispatches", default=None
)


def _get_or_init() -> list[Callable[[], Any]]:
    lst = _pending.get()
    if lst is None:
        lst = []
        _pending.set(lst)
    return lst


def add_pending_dispatch(fn: Callable[[], Any]) -> None:
    """Register a dispatch callable to run after the current UoW commits.

    In Celery mode ``fn`` is a plain sync callable (``send_task`` is sync).
    In inline mode ``fn`` is an async coroutine method.
    :func:`flush_pending_dispatches` handles both transparently.

    Args:
        fn: Zero-argument callable.  May return ``None`` (sync) or a
            coroutine (async).
    """
    _get_or_init().append(fn)


async def flush_pending_dispatches() -> None:
    """Execute all pending dispatches and clear the queue.

    Called by :class:`~loom.core.engine.executor.RuntimeExecutor` after a
    successful UoW commit.  Clears the queue before executing so a dispatch
    that raises does not leave stale entries for the next request.

    Sync callables (Celery ``send_task``) are called directly.
    Async callables (inline runner) are awaited.
    """
    fns = _pending.get()
    _pending.set(None)
    if not fns:
        return
    for fn in fns:
        result = fn()
        if inspect.iscoroutine(result):
            await result


def clear_pending_dispatches() -> None:
    """Discard all pending dispatches without executing them.

    Called by the executor after a UoW rollback.  Jobs registered during
    a failed transaction must not be sent to the broker.
    """
    _pending.set(None)
