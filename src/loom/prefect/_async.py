"""Shared helper to run a Prefect-client coroutine from sync code.

Several call sites in ``loom.prefect`` (failure hooks, the synthetic
task-run observer) need to invoke async Prefect API calls from sync
contexts (Prefect ``on_failure`` hooks, loom executor callbacks). Each
used to spin its own ``asyncio.new_event_loop()`` which breaks when an
outer loop is already running (e.g. async flow workers).

Prefer ``prefect.utilities.asyncutils.run_coro_as_sync`` because it
handles both cases (active loop vs. no loop) without crashing.
"""

from __future__ import annotations

from typing import Any, TypeVar

from prefect.utilities.asyncutils import run_coro_as_sync

T = TypeVar("T")


def run_sync(coro: Any) -> Any:
    """Run *coro* to completion from a sync context.

    Delegates to Prefect's ``run_coro_as_sync`` so the call works
    whether or not an event loop is already running in the current
    thread.

    Args:
        coro: An awaitable returned by an ``async def`` function.

    Returns:
        The value the coroutine returned.
    """
    return run_coro_as_sync(coro)


__all__ = ["run_sync"]
