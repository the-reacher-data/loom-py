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
validated URL and its credential — live here once, so start-up cannot validate
a server the run would not reach.  The credential itself is resolved by
:mod:`loom.ai.remote_auth`, which is where the deployment's own strategy plugs in;
this module only carries the result to the client.

The MCP client ships as an optional ``pydantic-ai-slim`` dependency, so it is
imported inside the function that needs it: importing it at module load would
break every deployment that declares no ``mcp`` grant.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, cast

from loom.ai.compiler import CompiledMcpCapability
from loom.ai.errors import AgentCompilationError, mcp_transport_invalid, provider_not_installed
from loom.ai.remote_auth import headers_from_ref, shared_mcp_auth

if TYPE_CHECKING:
    from fastmcp.client.transports import ClientTransport
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

    The credential is applied exactly as the deployment declared it: fixed
    headers from ``headers_ref``, or the object the named strategy builds —
    one instance per server, shared by every agent granted it.  A server that
    declares neither is connected exactly as before, which is what lets one
    artifact move between environments unchanged.

    Args:
        capability: Compiled grant carrying the validated address of its
            transport and the credential resolved for it.

    Returns:
        The unfiltered, not yet connected toolset; the caller applies the
        grant's tool filter and the capability boundary.

    Raises:
        AgentCompilationError: When the MCP client is not installed, the
            grant's transport is not one this engine serves, the
            ``headers_ref`` payload is not one ``Name=value`` pair, or the
            named strategy cannot be built.

    Example::

        toolset = build_mcp_toolset(capability)
    """
    try:
        from pydantic_ai.mcp import MCPToolset
    except ImportError as exc:
        raise AgentCompilationError([provider_not_installed("mcp", "mcp")]) from exc
    component = f"mcp server '{capability.server}'"
    client = _mcp_client(component, capability)
    headers = headers_from_ref(component, capability.headers_ref) or None
    auth = shared_mcp_auth(capability.server, capability.auth)
    # ``MCPToolset`` annotates auth as ``httpx.Auth | Literal['oauth'] | str | None``,
    # which admits no callable, while what really consumes it is fastmcp's HTTP
    # transport: its ``_set_auth`` special-cases only ``"oauth"``, ``OAuth``, the
    # OAuth providers and ``str``, and hands anything else to its ``httpx2`` client
    # untouched — including the callable loom's own strategies return.
    toolset: MCPToolset[Any] = MCPToolset(client, headers=headers, auth=cast("Any", auth))
    return toolset


def _mcp_client(component: str, capability: CompiledMcpCapability) -> str | ClientTransport:
    """Return what ``MCPToolset`` connects to for the grant's transport.

    Under ``http`` that is the validated server URL.  Under ``stdio`` it is a
    transport that spawns the declared command, receives only the declared
    environment and dies with the context that opened it, so no server outlives
    the toolset that owns it.

    Raises:
        AgentCompilationError: When the transport is not one this engine serves,
            or the stdio client library is missing.
    """
    if capability.transport == "http" and capability.url is not None:
        return capability.url
    if capability.transport == "stdio" and capability.command is not None:
        return _stdio_transport(capability.command, capability)
    reason = f"transport {capability.transport!r} is not served by the pydantic-ai engine"
    raise AgentCompilationError([mcp_transport_invalid(component, reason)])


def _stdio_transport(command: str, capability: CompiledMcpCapability) -> ClientTransport:
    """Build the stdio transport of one grant, tied to its owner's lifetime.

    Raises:
        AgentCompilationError: When the stdio client library is not installed.
    """
    try:
        from fastmcp.client.transports import StdioTransport
    except ImportError as exc:
        raise AgentCompilationError([provider_not_installed("mcp", "mcp")]) from exc
    return StdioTransport(
        command=command,
        args=list(capability.args),
        env=dict(capability.env) or None,
        keep_alive=False,
    )


@asynccontextmanager
async def create_mcp_client(capability: CompiledMcpCapability) -> AsyncIterator[_ToolsetSession]:
    """Open one session against an MCP server, connected and ready to list.

    Satisfies :data:`~loom.ai.runtime.McpClientFactory`: nothing happens until
    the context is entered, so the runtime's start-up deadline bounds the whole
    of it and a failure is reported as a coded start-up issue rather than as an
    exception escaping ``create_app``.

    Args:
        capability: Compiled grant carrying the validated address of its transport.

    Yields:
        The connected session, closed with its transport on exit.

    Raises:
        AgentCompilationError: When the MCP client is not installed, or the
            grant's credential cannot be resolved.

    Example::

        runtime = AgentRuntime(..., mcp_client_factory=create_mcp_client)
    """
    toolset = build_mcp_toolset(capability)
    async with toolset:
        yield _ToolsetSession(toolset)
