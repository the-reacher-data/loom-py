"""Tests for the post-commit channel (spec 005, design D1)."""

from __future__ import annotations

import asyncio

import pytest

from loom.core.engine.post_commit import (
    PostCommitChannel,
    PostCommitError,
    active_channel,
    bind_channel,
    channel_bound,
    reset_channel,
)
from loom.core.errors import LoomError


async def test_drain_runs_actions_in_enqueue_order() -> None:
    calls: list[str] = []
    channel = PostCommitChannel()
    channel.enqueue(lambda: calls.append("a"))
    channel.enqueue(lambda: calls.append("b"))
    channel.enqueue(lambda: calls.append("c"))

    await channel.drain()

    assert calls == ["a", "b", "c"]


async def test_drain_awaits_sync_returning_awaitable_and_async_callables() -> None:
    calls: list[str] = []

    async def _async_action() -> None:
        await asyncio.sleep(0)
        calls.append("async")

    def _sync_returning_awaitable() -> asyncio.Future[None]:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        loop.call_soon(lambda: (calls.append("future"), future.set_result(None)))
        return future

    channel = PostCommitChannel()
    channel.enqueue(_async_action)
    channel.enqueue(_sync_returning_awaitable)
    channel.enqueue(lambda: calls.append("sync"))

    await channel.drain()

    assert calls == ["async", "future", "sync"]


async def test_drain_continues_after_failure_then_raises_with_all_failures() -> None:
    calls: list[str] = []
    first = RuntimeError("first")
    second = ValueError("second")

    def _fail(exc: Exception) -> None:
        raise exc

    channel = PostCommitChannel()
    channel.enqueue(lambda: _fail(first))
    channel.enqueue(lambda: calls.append("middle"))
    channel.enqueue(lambda: _fail(second))
    channel.enqueue(lambda: calls.append("last"))

    with pytest.raises(PostCommitError) as info:
        await channel.drain()

    assert calls == ["middle", "last"]
    assert info.value.committed is True
    assert info.value.failures == (first, second)
    assert isinstance(info.value, LoomError)
    assert info.value.code == "post_commit_failure"
    assert channel.is_empty


async def test_channel_is_empty_after_drain() -> None:
    channel = PostCommitChannel()
    channel.enqueue(lambda: None)
    assert not channel.is_empty

    await channel.drain()

    assert channel.is_empty
    await channel.drain()  # a second drain is a no-op


def test_discard_drops_actions_without_running_them() -> None:
    calls: list[str] = []
    channel = PostCommitChannel()
    channel.enqueue(lambda: calls.append("never"))

    channel.discard()

    assert channel.is_empty
    assert calls == []


async def test_channel_is_unbound_during_drain_and_restored_after() -> None:
    seen: list[PostCommitChannel | None] = []
    channel = PostCommitChannel()
    channel.enqueue(lambda: seen.append(active_channel()))
    token = bind_channel(channel)
    try:
        assert channel_bound()
        await channel.drain()
        assert active_channel() is channel
    finally:
        reset_channel(token)

    assert seen == [None]
    assert not channel_bound()


async def test_nested_drain_from_an_action_does_not_recurse() -> None:
    calls: list[str] = []
    channel = PostCommitChannel()

    async def _drain_again() -> None:
        calls.append("outer")
        await channel.drain()

    channel.enqueue(_drain_again)
    channel.enqueue(lambda: calls.append("second"))

    await channel.drain()

    assert calls == ["outer", "second"]


def test_bind_and_reset_restore_previous_channel() -> None:
    outer = PostCommitChannel()
    inner = PostCommitChannel()
    outer_token = bind_channel(outer)
    inner_token = bind_channel(inner)

    assert active_channel() is inner
    reset_channel(inner_token)
    assert active_channel() is outer
    reset_channel(outer_token)
    assert active_channel() is None


async def test_cancellation_propagates_and_keeps_the_rest_queued() -> None:
    calls: list[str] = []
    channel = PostCommitChannel()

    def _cancel() -> None:
        raise asyncio.CancelledError()

    channel.enqueue(_cancel)
    channel.enqueue(lambda: calls.append("after"))
    token = bind_channel(channel)
    try:
        with pytest.raises(asyncio.CancelledError):
            await channel.drain()
        assert active_channel() is channel
    finally:
        reset_channel(token)

    assert calls == []
    assert not channel.is_empty
    await channel.drain()
    assert calls == ["after"]
