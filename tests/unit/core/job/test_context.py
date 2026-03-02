from __future__ import annotations

import asyncio

from loom.core.job.context import (
    add_pending_dispatch,
    clear_pending_dispatches,
    flush_pending_dispatches,
)


async def test_add_and_flush_sync_callable() -> None:
    calls: list[str] = []
    add_pending_dispatch(lambda: calls.append("a"))
    add_pending_dispatch(lambda: calls.append("b"))
    await flush_pending_dispatches()
    assert calls == ["a", "b"]


async def test_flush_clears_queue() -> None:
    add_pending_dispatch(lambda: None)
    await flush_pending_dispatches()
    # second flush should be a no-op
    calls: list[str] = []
    add_pending_dispatch(lambda: calls.append("x"))
    await flush_pending_dispatches()
    assert calls == ["x"]


async def test_flush_empty_queue_is_noop() -> None:
    await flush_pending_dispatches()  # must not raise


async def test_flush_async_callable_is_awaited() -> None:
    calls: list[str] = []

    async def _async_fn() -> None:
        calls.append("async")

    add_pending_dispatch(_async_fn)
    await flush_pending_dispatches()
    assert calls == ["async"]


async def test_clear_discards_without_executing() -> None:
    calls: list[str] = []
    add_pending_dispatch(lambda: calls.append("should-not-run"))
    clear_pending_dispatches()
    await flush_pending_dispatches()
    assert calls == []


async def test_contextvar_isolated_between_tasks() -> None:
    """Each asyncio task must have its own independent pending queue."""
    task_calls: dict[str, list[str]] = {"a": [], "b": []}

    async def task_a() -> None:
        add_pending_dispatch(lambda: task_calls["a"].append("a"))
        await flush_pending_dispatches()

    async def task_b() -> None:
        add_pending_dispatch(lambda: task_calls["b"].append("b"))
        await flush_pending_dispatches()

    await asyncio.gather(task_a(), task_b())

    assert task_calls["a"] == ["a"]
    assert task_calls["b"] == ["b"]


async def test_contextvar_default_not_shared() -> None:
    """Appending in one context must not affect a fresh context."""
    add_pending_dispatch(lambda: None)

    calls: list[str] = []

    async def fresh_context() -> None:
        # A new task starts with default=None, not the parent's list.
        await flush_pending_dispatches()
        add_pending_dispatch(lambda: calls.append("fresh"))
        await flush_pending_dispatches()

    task = asyncio.ensure_future(fresh_context())
    await task
    assert calls == ["fresh"]
