"""Pending job dispatches, delegated to the post-commit channel.

The public names are kept for existing callers; the queue itself is the
:class:`~loom.core.engine.post_commit.PostCommitChannel` bound by the
executor.  When no channel is bound, a per-context fallback channel holds
the dispatches so :func:`flush_pending_dispatches` and
:func:`clear_pending_dispatches` keep working for hand-driven callers.
"""

from __future__ import annotations

from collections.abc import Callable
from contextvars import ContextVar
from typing import Any

from loom.core.engine.post_commit import PostCommitChannel, active_channel

# default=None, never a channel instance: a shared default would leak
# dispatches across contexts.  Each context creates its own on first use.
_fallback: ContextVar[PostCommitChannel | None] = ContextVar(
    "_loom_pending_dispatches", default=None
)


def _fallback_channel() -> PostCommitChannel:
    channel = _fallback.get()
    if channel is None:
        channel = PostCommitChannel()
        _fallback.set(channel)
    return channel


def add_pending_dispatch(fn: Callable[[], Any]) -> None:
    """Register a dispatch callable to run after the current UoW commits.

    In Celery mode ``fn`` is a plain sync callable (``send_task`` is sync).
    In inline mode ``fn`` is an async coroutine method.
    :func:`flush_pending_dispatches` handles both transparently.

    Args:
        fn: Zero-argument callable.  May return ``None`` (sync) or a
            coroutine (async).
    """
    channel = active_channel() or _fallback_channel()
    channel.enqueue(fn)


async def flush_pending_dispatches() -> None:
    """Execute all pending dispatches of the fallback channel and clear it.

    Sync callables (Celery ``send_task``) are called directly.
    Async callables (inline runner) are awaited.

    Raises:
        loom.core.engine.post_commit.PostCommitError: If any dispatch raised;
            the remaining dispatches still run.
    """
    channel = _fallback.get()
    if channel is None:
        return
    await channel.drain()


def clear_pending_dispatches() -> None:
    """Discard all pending dispatches of the fallback channel without executing them.

    Jobs registered during a failed transaction must not be sent to the broker.
    """
    channel = _fallback.get()
    if channel is not None:
        channel.discard()
