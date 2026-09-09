"""Outbound MCP transport: this agent calling a remote tool server.

One ``MCPToolset`` per connection serves the whole worker, and
:class:`SharedMcpToolsets` is what holds them:

* :meth:`SharedMcpToolsets.open` is the
  :data:`~loom.ai.runtime.McpClientFactory` the runtime opens at start-up.
  Entering it connects the server and hands back a session, so an unreachable
  server fails start-up as ``MCP_SERVER_UNREACHABLE`` and the declared tool
  filters are validated against the tools the server really exposes (FR-025)
  instead of being taken on trust.
* :meth:`SharedMcpToolsets.toolset` is what the engine puts behind the
  capability boundary for the run itself — the *same object*, so the start-up
  connection is the run's connection and a worker holds one channel per server
  rather than one per agent (FR-026).

What is shared is the toolset, never a
:class:`~loom.ai.runtime.SharedMcpSession`: that wrapper serialises every call
on one lock, so routing runs through it would queue every agent's tool call
behind every other agent's. ``MCPToolset`` instead reference-counts its
entries, so concurrent agents keep their calls in parallel over one session.

:func:`build_mcp_toolset` holds the connection rules of one grant — its
validated URL and its credential — once, so start-up cannot validate a server
the run would not reach.  The credential itself is resolved by
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

from loom.ai.abc import McpToolCallResult, McpToolInfo
from loom.ai.compiler import CompiledMcpCapability, mcp_connection
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

    async def list_tools(self) -> tuple[McpToolInfo, ...]:
        """Return the tools the server exposes, each with its output-schema flag.

        ``MCPToolset.list_tools`` caches its result (``cache_tools=True`` by
        default), so a second call from
        :class:`~loom.ai.runtime._grants.McpGrantView` costs no extra round
        trip beyond the one start-up already pays.
        """
        return tuple(
            McpToolInfo(name=tool.name, has_output_schema=tool.output_schema is not None)
            for tool in await self._toolset.list_tools()
        )

    async def call_tool(self, name: str, arguments: Mapping[str, Any]) -> McpToolCallResult:
        """Invoke one tool and return its protocol-level result.

        Goes straight to the underlying client's ``call_tool_mcp`` rather than
        ``MCPToolset.direct_call_tool``: the latter is written for the model's
        own retry loop and turns an ``is_error`` result into a raised
        ``ModelRetry``/``ToolFailed`` before the caller ever sees the flag.
        This adapter's caller is
        :class:`~loom.ai.runtime._grants.McpGrantView`, which must inspect
        ``ok`` itself — before deciding whether to decode — so the raw
        protocol result is what it needs, with neither prose-fallback mapping
        nor an engine-facing exception in the way.

        Args:
            name: Tool name as the server exposes it.
            arguments: Arguments to pass to the tool.

        Returns:
            The server's own error flag and structured content, verbatim.
        """
        async with self._toolset:
            result = await self._toolset.client.call_tool_mcp(name, dict(arguments))
        return McpToolCallResult(ok=not result.is_error, structured=result.structured_content)


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


def _unopened_connection(agent: str, server: str) -> str:
    """Describe the wiring that would give an agent an unvalidated connection."""
    return (
        f"{agent}: the runtime never opened mcp server '{server}' through this engine's "
        f"shared toolsets. The runtime's 'mcp_client_factory' must be the "
        f"'mcp_client_factory' of the very provider instance that builds the engines, "
        f"or the run would hold a second connection whose tool filters start-up never "
        f"validated"
    )


class SharedMcpToolsets:
    """The MCP toolsets of one worker: one per distinct connection.

    Every agent granted a server works over the same ``MCPToolset`` instance,
    which is what makes a worker hold one channel per server instead of one
    per agent. ``MCPToolset`` reference-counts ``__aenter__``, so the runtime
    entering it at start-up and each run entering it again open a single
    session, and the connection closes when the last holder leaves.

    Sharing the toolset — rather than a
    :class:`~loom.ai.runtime.SharedMcpSession` over it — is what keeps the
    runs concurrent: the session wrapper serialises on one lock and drains a
    cancelled call while still holding it, so one agent's ``tool_timeout_ms``
    would stop bounding anything for its neighbours.

    All of that holds only while the runtime's ``mcp_client_factory`` and the
    engine build read the *same* instance. :meth:`open` records every
    connection it was asked for and :meth:`for_build` refuses one it was never
    asked for, which is why the two are separate methods: a shared
    build-and-cache helper would have made :meth:`open` satisfy its own check
    and left the wiring unverified. The engine build runs strictly after the
    runtime opened its clients, so a grant missing from that record can only
    mean the runtime was handed some other factory — and the run would then
    hold a second connection whose tool filters nothing validated (FR-025).

    State is per instance and owned by the engine provider; nothing here is
    module-level. One instance serves one runtime lifecycle: the toolsets are
    never evicted, and an ``MCPToolset`` re-entered from a second event loop
    would reuse a lock bound to the first.

    Example::

        shared = SharedMcpToolsets()
        async with shared.open(capability) as session:
            names = await session.list_tools()
    """

    def __init__(self) -> None:
        self._toolsets: dict[CompiledMcpCapability, MCPToolset[Any]] = {}
        self._opened: set[CompiledMcpCapability] = set()

    def for_build(self, capability: CompiledMcpCapability, agent: str) -> MCPToolset[Any]:
        """Return the shared toolset an agent's ``mcp`` grant must run over.

        The toolset is keyed by connection alone
        (:func:`~loom.ai.compiler.mcp_connection`), so two agents that filter
        one server differently share it; each applies its own filter on top,
        which changes neither the shared object nor the other agent's view.

        Args:
            capability: Compiled grant naming the connection.
            agent: Plan being built, named in the wiring failure.

        Returns:
            The worker's toolset for that connection, already opened by the
            runtime.

        Raises:
            AgentCompilationError: When the runtime never opened this
                connection through :meth:`open` — the deployment wired some
                other MCP client factory — or when the toolset cannot be built.
        """
        connection = mcp_connection(capability)
        if connection not in self._opened:
            raise AgentCompilationError([_unopened_connection(agent, capability.server)])
        return self._toolset(connection)

    def _toolset(self, connection: CompiledMcpCapability) -> MCPToolset[Any]:
        """Return the connection's toolset, building it at most once."""
        existing = self._toolsets.get(connection)
        if existing is not None:
            return existing
        built = build_mcp_toolset(connection)
        self._toolsets[connection] = built
        return built

    @asynccontextmanager
    async def open(self, capability: CompiledMcpCapability) -> AsyncIterator[_ToolsetSession]:
        """Open the worker's session for one grant, connected and ready to list.

        Satisfies :data:`~loom.ai.runtime.McpClientFactory`: nothing happens
        until the context is entered, so the runtime's start-up deadline bounds
        the whole of it and a failure is reported as a coded start-up issue
        rather than as an exception escaping ``create_app``. Holding the
        context open is what keeps the shared connection alive for the runs.

        Args:
            capability: Compiled grant carrying the validated address of its
                transport.

        Yields:
            The connected session; leaving releases this holder's reference and
            closes the transport once no run holds one either.

        Raises:
            AgentCompilationError: When the MCP client is not installed, or the
                grant's credential cannot be resolved.
        """
        connection = mcp_connection(capability)
        self._opened.add(connection)
        toolset = self._toolset(connection)
        async with toolset:
            yield _ToolsetSession(toolset)


@asynccontextmanager
async def create_mcp_client(capability: CompiledMcpCapability) -> AsyncIterator[_ToolsetSession]:
    """Open one throw-away session against an MCP server, outside any sharing.

    Kept for a caller that needs a session of its own — a diagnostic, a probe,
    a test. A composition root must **not** wire this as the runtime's
    ``mcp_client_factory``: it builds a toolset the engine's run path does not
    know about, so the worker would hold that connection plus one per agent.
    Read ``mcp_client_factory`` off the engine provider instead, which is what
    ``create_app`` does.

    Args:
        capability: Compiled grant carrying the validated address of its transport.

    Yields:
        The connected session, closed with its transport on exit.

    Raises:
        AgentCompilationError: When the MCP client is not installed, or the
            grant's credential cannot be resolved.

    Example::

        async with create_mcp_client(capability) as session:
            names = await session.list_tools()
    """
    toolset = build_mcp_toolset(capability)
    async with toolset:
        yield _ToolsetSession(toolset)
