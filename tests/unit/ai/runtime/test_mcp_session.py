"""A session declares whether it needs serialisation; the runtime stops deciding it.

``mcp_session_for`` is the only point that decides whether to wrap a session in
``SharedMcpSession``: it does so by default, and leaves the session as-is when it
declares :class:`~loom.ai.abc.ConcurrentMcpSession`. These tests measure what
matters -- that several concurrent calls on a declared session really do run in
parallel, not merely that the lock is gone -- and pin the opposite: an
undeclared session keeps serialising. They also cover cancellation in both
shapes, which does not behave the same way: ``SharedMcpSession`` still
drains the cancelled call before releasing its lock, so its neighbours on the
same session remain usable; a session declared concurrent holds no lock to
protect, so cancelling it returns to the caller immediately instead of
draining.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from types import CoroutineType
from typing import Any

from loom.ai.abc import ConcurrentMcpSession, McpSession, McpToolCallResult, McpToolInfo
from loom.ai.runtime._mcp import SharedMcpSession, mcp_session_for

_CALL_DELAY = 0.05
"""One call's own duration; long enough that N serial calls are measurably
slower than N concurrent ones, short enough to keep the suite fast."""


class _DelayedSession:
    """A plain ``McpSession`` double: declares nothing, so it keeps queuing."""

    def __init__(self, *, delay: float = _CALL_DELAY) -> None:
        self._delay = delay
        self.finished: list[str] = []

    async def list_tools(self) -> tuple[McpToolInfo, ...]:
        return ()

    async def call_tool(self, name: str, arguments: Mapping[str, Any]) -> McpToolCallResult:
        await asyncio.sleep(self._delay)
        self.finished.append(name)
        return McpToolCallResult(ok=True, structured=None)


class _CountingSession:
    """Counts every ``call_tool`` invocation -- i.e. every coroutine built.

    ``call_tool`` is a plain function returning a coroutine, not an
    ``async def`` itself: calling it builds and returns the coroutine
    object synchronously, without running any of its body, so the counter
    must go up in ``call_tool`` itself rather than in the coroutine it
    hands back. Bumping it inside an ``async def`` body would only go up
    once the coroutine is awaited -- exactly what must not happen for a
    caller still queued behind :class:`SharedMcpSession`'s lock, and
    exactly the bug a doubly-anticipatory coroutine would hide.
    """

    def __init__(self) -> None:
        self.built = 0

    async def list_tools(self) -> tuple[McpToolInfo, ...]:
        return ()

    def call_tool(
        self, name: str, arguments: Mapping[str, Any]
    ) -> CoroutineType[Any, Any, McpToolCallResult]:
        self.built += 1
        return self._run()

    async def _run(self) -> McpToolCallResult:
        await asyncio.sleep(_CALL_DELAY)
        return McpToolCallResult(ok=True, structured=None)


class _DelayedConcurrentSession(_DelayedSession, ConcurrentMcpSession):
    """The same double, declaring itself already safe for concurrent calls.

    Inherits :meth:`_DelayedSession.call_tool` unchanged: a session declaring
    :class:`~loom.ai.abc.ConcurrentMcpSession` must **not** shield a call from
    its own caller's cancellation — there is no shared frame left to
    desynchronise, so nothing here should drain.
    """


async def _call_many(session: McpSession, count: int) -> float:
    start = time.monotonic()
    await asyncio.gather(*(session.call_tool(f"tool{i}", {}) for i in range(count)))
    return time.monotonic() - start


class TestConcurrencyDeclaration:
    """``mcp_session_for`` reads the session's declaration, it does not decide for it."""

    async def test_a_session_declaring_concurrency_is_left_unwrapped(self) -> None:
        session = _DelayedConcurrentSession()
        assert mcp_session_for(session, label="crm") is session

    async def test_a_session_declaring_nothing_is_wrapped_in_shared(self) -> None:
        session = _DelayedSession()
        wrapped = mcp_session_for(session, label="crm")
        assert isinstance(wrapped, SharedMcpSession)
        assert wrapped is not session


class TestRealParallelism:
    """The success criterion is not that the lock disappears: it is that N
    concurrent calls on one grant really do run in parallel. The "declared"
    side of that criterion is not measured by a double with no lock of
    its own -- ``_DelayedConcurrentSession`` runs in parallel by
    construction, and nothing in production can turn it red -- but by the
    real path, which crosses ``mcp_session_for`` and ``_ToolsetSession``
    (``tests/unit/ai/engines/test_pydantic_ai_mcp_concurrency.py``)."""

    async def test_an_undeclared_session_keeps_serialising(self) -> None:
        session = _DelayedSession()
        wrapped = mcp_session_for(session, label="crm")
        elapsed = await _call_many(wrapped, 8)
        # Eight calls in a row look like eight, not one.
        assert elapsed > _CALL_DELAY * 6


class TestDrainingOnCancellation:
    """A call cancelled halfway through ``SharedMcpSession`` must not
     desynchronise its neighbours: it drains before releasing the lock. A
     session declared concurrent holds no lock to protect, so its
     cancellation returns to the caller immediately instead of draining
    ."""

    async def test_shared_mcp_session_drains_the_cancelled_call(self) -> None:
        session = _DelayedSession(delay=0.1)
        shared = SharedMcpSession(session, label="crm")
        task = asyncio.create_task(shared.call_tool("victim", {}))
        await asyncio.sleep(0.01)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("expected the cancelled call to re-raise")
        # Drained before releasing the lock: it should already count as finished.
        assert session.finished == ["victim"]
        # And the lock is still usable for the next neighbour.
        await shared.call_tool("neighbour", {})
        assert session.finished == ["victim", "neighbour"]

    async def test_a_concurrent_session_does_not_drain_on_cancellation(self) -> None:
        session = _DelayedConcurrentSession(delay=0.1)
        task = asyncio.create_task(session.call_tool("victim", {}))
        await asyncio.sleep(0.01)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("expected the cancelled call to re-raise")
        # Order, not the clock: with no lock to hold, the cancellation has
        # already returned to the caller -- and with no shield, the
        # abandoned call never reaches completion.
        assert session.finished == []

    async def test_a_concurrent_neighbour_is_not_blocked_when_the_other_is_cancelled(
        self,
    ) -> None:
        session = _DelayedConcurrentSession(delay=0.1)
        start = time.monotonic()
        victim = asyncio.create_task(session.call_tool("victim", {}))
        neighbour = asyncio.create_task(session.call_tool("neighbour", {}))
        await asyncio.sleep(0.01)
        victim.cancel()
        await neighbour
        elapsed = time.monotonic() - start
        # The neighbour was running in parallel from the start: it finishes
        # around its own delay, not double it -- which is what it would take
        # if it had been queued behind the cancelled call's draining.
        assert elapsed < 0.15
        try:
            await victim
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("expected the cancelled call to re-raise")


class TestTheCoroutineIsBuiltWithTheLockAlreadyHeld:
    """A caller cancelled while waiting its turn must never leave a coroutine
    of its own unexecuted forever (H6): ``SharedMcpSession`` only builds the
    next turn's call once it already holds the lock."""

    async def test_a_queued_caller_does_not_build_its_coroutine_if_cancelled(self) -> None:
        session = _CountingSession()
        shared = SharedMcpSession(session, label="crm")
        holder = asyncio.create_task(shared.call_tool("holder", {}))
        await asyncio.sleep(0)
        queued = asyncio.create_task(shared.call_tool("queued", {}))
        await asyncio.sleep(0)
        # ``queued`` is still waiting for the lock: cancelling it here must
        # not have already built its own ``call_tool`` coroutine.
        queued.cancel()
        try:
            await queued
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("expected the queued call to re-raise")
        await holder
        # Only the one holding the lock got to build its coroutine.
        assert session.built == 1
