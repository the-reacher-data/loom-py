"""Outbound MCP transport: this agent calling a remote tool server.

Two entry points, one connection recipe — the shape :mod:`._a2a` already uses:

* :func:`create_mcp_client` is the :data:`~loom.ai.runtime.McpClientFactory`
  the runtime opens at start-up. Entering it connects the server and hands back
  a session, so an unreachable server fails start-up as
  ``MCP_SERVER_UNREACHABLE`` and the declared tool filters are validated
  against the tools the server really exposes (FR-025) instead of being taken
  on trust.
* :func:`build_mcp_toolset` is what the engine puts behind the capability
  boundary for the run itself.

Both build the same ``MCPToolset``: the connection rules of one grant — its
validated URL, and the refusal of a ``headers_ref`` the engine cannot resolve —
live here once, so start-up cannot validate a server the run would not reach.

The MCP client ships as an optional ``pydantic-ai-slim`` dependency, so it is
imported inside the function that needs it: importing it at module load would
break every deployment that declares no ``mcp`` grant.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from loom.ai.compiler import CompiledMcpCapability
from loom.ai.errors import AgentCompilationError, provider_not_installed

if TYPE_CHECKING:
    from pydantic_ai.mcp import MCPToolset


class _ToolsetSession:
    """Adapts a connected ``MCPToolset`` to the runtime's session contract.

    The runtime asks a session for two things only — the tool names a server
    exposes, and one tool call — so the adapter is the narrowing of the
    toolset's much wider surface down to
    :class:`~loom.ai.runtime.McpSession`.

    Args:
        toolset: Already-entered toolset speaking to one server.
    """

    def __init__(self, toolset: MCPToolset[Any]) -> None:
        self._toolset = toolset

    async def list_tools(self) -> tuple[str, ...]:
        """Return the tool names the server exposes."""
        return tuple(tool.name for tool in await self._toolset.list_tools())

    async def call_tool(self, name: str, arguments: Mapping[str, Any]) -> object:
        """Invoke one tool and return its result.

        Args:
            name: Tool name as the server exposes it.
            arguments: Arguments to pass to the tool.

        Returns:
            The tool's result, as the client library decoded it.
        """
        return await self._toolset.direct_call_tool(name, dict(arguments))


def build_mcp_toolset(capability: CompiledMcpCapability) -> MCPToolset[Any]:
    """Build the unfiltered toolset of one grant, applying its connection rules.

    Args:
        capability: Compiled grant carrying the validated server URL.

    Returns:
        The unfiltered, not yet connected toolset; the caller applies the
        grant's tool filter and the capability boundary.

    Raises:
        AgentCompilationError: When the MCP client is not installed, or the
            grant carries a ``headers_ref`` no deployment secret resolver
            reaches from here.

    Example::

        toolset = build_mcp_toolset(capability)
    """
    try:
        from pydantic_ai.mcp import MCPToolset
    except ImportError as exc:
        raise AgentCompilationError([provider_not_installed("mcp", "mcp")]) from exc
    if capability.headers_ref is not None:
        raise AgentCompilationError(
            [
                f"mcp server '{capability.server}': headers_ref cannot be resolved by "
                f"the engine; the deployment secret resolver does not reach it"
            ]
        )
    toolset: MCPToolset[Any] = MCPToolset(capability.url)
    return toolset


@asynccontextmanager
async def create_mcp_client(capability: CompiledMcpCapability) -> AsyncIterator[_ToolsetSession]:
    """Open one session against an MCP server, connected and ready to list.

    Satisfies :data:`~loom.ai.runtime.McpClientFactory`: nothing happens until
    the context is entered, so the runtime's start-up deadline bounds the whole
    of it and a failure is reported as a coded start-up issue rather than as an
    exception escaping ``create_app``.

    Args:
        capability: Compiled grant carrying the validated server URL.

    Yields:
        The connected session, closed with its transport on exit.

    Raises:
        AgentCompilationError: When the MCP client is not installed, or the
            grant carries an unresolvable ``headers_ref``.

    Example::

        runtime = AgentRuntime(..., mcp_client_factory=create_mcp_client)
    """
    toolset = build_mcp_toolset(capability)
    async with toolset:
        yield _ToolsetSession(toolset)
