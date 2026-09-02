"""MCP sessions shared per server, and the tool filters checked against them.

A JSON-RPC session is a single framed stream, so one shared session per server
serialises the calls of every concurrent run. The declared tool filters are
validated here too (FR-025): they are checked against the tools a server really
lists, which is a property of the session, not of the runtime lifecycle.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine, Iterable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Any, Protocol, TypeVar

from loom.ai._filters import select_names
from loom.ai.compiler._plan import AgentPlan, CompiledMcpCapability
from loom.ai.errors import (
    AgentCompilationIssue,
    mcp_server_unreachable,
    tool_filter_matches_nothing,
)

_logger = logging.getLogger(__name__)

_T = TypeVar("_T")


class McpSession(Protocol):
    """Minimal MCP session the runtime needs from any client library."""

    async def list_tools(self) -> tuple[str, ...]:
        """Return the tool names the server exposes.

        Returns:
            Every tool name the server advertises, before any declared filter
            is applied.
        """
        ...

    async def call_tool(self, name: str, arguments: Mapping[str, Any]) -> object:
        """Invoke one tool and return its result.

        Args:
            name: Tool name as the server exposes it.
            arguments: Arguments to pass to the tool.

        Returns:
            The tool's result, as the client library decoded it.
        """
        ...


McpClientFactory = Callable[[CompiledMcpCapability], AbstractAsyncContextManager[McpSession]]
"""Builds the (not yet opened) client of one compiled MCP capability."""


class SharedMcpSession:
    """Serialises every call to one MCP session shared by concurrent runs.

    A JSON-RPC session is a single framed stream: two overlapping calls
    interleave their frames, and a caller cancelled mid-frame leaves the
    session desynchronised for its neighbours. Both are prevented here — one
    lock per session, and the in-flight call shielded and drained to
    completion before the lock is released, after which the cancellation is
    re-raised to the caller that asked for it.

    Args:
        session: The live session to guard.
        label: Human-readable name used in log messages.
    """

    def __init__(self, session: McpSession, *, label: str) -> None:
        self._session = session
        self._label = label
        self._lock = asyncio.Lock()

    async def list_tools(self) -> tuple[str, ...]:
        """Return the tool names the server exposes, serialised with every other call.

        Returns:
            Every tool name the underlying session advertises.
        """
        return await self._serialised(self._session.list_tools())

    async def call_tool(self, name: str, arguments: Mapping[str, Any]) -> object:
        """Invoke one tool, serialised with every other call on this session.

        Args:
            name: Tool name as the server exposes it.
            arguments: Arguments to pass to the tool.

        Returns:
            The tool's result.

        Raises:
            asyncio.CancelledError: When the caller is cancelled. The in-flight
                call still runs to completion, so the session stays usable.
        """
        return await self._serialised(self._session.call_tool(name, arguments))

    async def _serialised(self, call: Coroutine[Any, Any, _T]) -> _T:
        async with self._lock:
            in_flight = asyncio.ensure_future(call)
            try:
                return await asyncio.shield(in_flight)
            except asyncio.CancelledError:
                _logger.debug("mcp session %r: draining a cancelled call", self._label)
                await asyncio.wait([in_flight])
                _discard_outcome(in_flight)
                raise


def _discard_outcome(task: asyncio.Future[Any]) -> None:
    """Consume a drained call's outcome so it is never reported as unretrieved."""
    if not task.cancelled():
        task.exception()


def _filtered_tools(
    tools: Sequence[str], *, include: Sequence[str], exclude: Sequence[str]
) -> tuple[str, ...]:
    """Apply the glob ``include`` then ``exclude`` to the tools a server offers."""
    return select_names(tools, include=include, exclude=exclude)


@dataclass(frozen=True, slots=True)
class FilterTarget:
    """One declared tool filter and the shared session it must be checked against."""

    agent: str
    server: str
    key: str
    include: tuple[str, ...]
    exclude: tuple[str, ...]


def filter_targets(plans: Iterable[AgentPlan]) -> tuple[FilterTarget, ...]:
    """Return every declared MCP tool filter, in plan then declaration order."""
    return tuple(
        FilterTarget(
            agent=plan.name,
            server=capability.server,
            key=mcp_key(capability),
            include=capability.include,
            exclude=capability.exclude,
        )
        for plan in plans
        for capability in plan.capabilities
        if type(capability) is CompiledMcpCapability and (capability.include or capability.exclude)
    )


def filter_issues(
    targets: Iterable[FilterTarget], listed: Mapping[str, tuple[str, ...]]
) -> list[AgentCompilationIssue]:
    """Return one issue per filter that selects none of its server's tools."""
    return [
        tool_filter_matches_nothing(target.agent, target.server)
        for target in targets
        if target.key in listed
        and not _filtered_tools(listed[target.key], include=target.include, exclude=target.exclude)
    ]


def listing_timeout_issues(
    targets: Iterable[FilterTarget], listed: Mapping[str, tuple[str, ...]]
) -> list[AgentCompilationIssue]:
    """Name every server whose tool listing did not complete inside the budget."""
    pending: dict[str, str] = {
        target.key: target.server for target in targets if target.key not in listed
    }
    return [
        mcp_server_unreachable(server, "listing its tools timed out") for server in pending.values()
    ]


def mcp_key(capability: CompiledMcpCapability) -> str:
    """Return the health-check key of one MCP capability, by registered name."""
    return f"mcp:{capability.server}"
