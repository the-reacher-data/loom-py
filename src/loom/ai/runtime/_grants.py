"""The grant views an :class:`~loom.ai.abc.AgentHandle` exposes (T301–T303).

``McpGrantView`` and ``SqlGrantView`` reach exactly what the artefact already
grants and what the runtime already opened — never a second connection and
never a second filter:

* :class:`McpGrantView` filters the tools this server's shared session
  advertises with the same include/exclude rule
  :func:`~loom.ai.engines.pydantic_ai._capabilities._tool_predicate` applies
  to the model's own toolset, over the same
  :class:`~loom.ai.runtime._mcp.SharedMcpSession`
  :class:`~loom.ai.runtime.AgentRuntime` opened at start-up. There is no
  second filter that could diverge from the model's, because there is no
  second filter.
* :class:`SqlGrantView` runs under the grant's own row and byte bounds, with
  roles resolved from the verified caller through
  :func:`~loom.core.sql.roles.resolve_query_roles` — the same call the
  model's own ``sql`` capability and :class:`~loom.core.sql.caller_bound.CallerBoundSql`
  make, with ``roles_bound=True`` and no caller-supplied role.

Both bound one call by the plan's own ``tool_timeout_ms``, inside a span —
the neutral counterpart of
:func:`~loom.ai.engines.pydantic_ai._guards.capability_call`, since this
package stays free of any one engine. What is deliberately **not** carried
over from the model-facing path is turning a failure into a refusal *value*
(:func:`~loom.ai.engines.pydantic_ai._guards.guarded`): that exists so a model
can read a refusal and try again, and the consumer of a grant view is
application code, which reads an exception instead.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import AbstractContextManager, asynccontextmanager, nullcontext
from typing import Any, TypeVar

import msgspec

from loom.ai._filters import matches
from loom.ai.abc import McpToolInfo
from loom.ai.compiler import CompiledMcpCapability, CompiledSqlCapability
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.runtime._mcp import SharedMcpSession
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from loom.core.observability.event import Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.sql.abc import RoleNotAllowedError, RolesNotBoundError, SqlQueryResult
from loom.core.sql.roles import resolve_query_roles
from loom.core.sql.service import SqlQueryService

_T = TypeVar("_T")


def _tool_allowed(name: str, capability: CompiledMcpCapability) -> bool:
    """Same include-then-exclude rule the model's own toolset filter applies."""
    if capability.include and not matches(name, capability.include):
        return False
    return not matches(name, capability.exclude)


class McpGrantView:
    """The concrete :class:`~loom.ai.abc.McpHandle` an ``AgentHandle.mcp()`` returns."""

    def __init__(
        self,
        *,
        agent: str,
        capability: CompiledMcpCapability,
        session: SharedMcpSession,
        catalogue: Sequence[McpToolInfo],
        timeout_s: float,
        identity: Identity,
        observability: ObservabilityRuntime | None,
    ) -> None:
        self._agent = agent
        self._capability = capability
        self._session = session
        self._identity = identity
        self._observability = observability
        self._timeout_s = timeout_s
        self._tools: dict[str, McpToolInfo] = {
            info.name: info for info in catalogue if _tool_allowed(info.name, capability)
        }

    def tools(self) -> tuple[str, ...]:
        """Return the tool names this grant's own filter admits."""
        return tuple(self._tools)

    async def call(self, tool: str, arguments: Mapping[str, Any], *, expect: type[_T]) -> _T:
        """Call *tool* and decode its structured result into *expect*."""
        info = self._require_tool(tool)
        if not info.has_output_schema:
            raise AgentRunError(
                AgentRunErrorCode.TOOL_UNTYPED,
                f"tool {tool!r} of mcp server {self._capability.server!r} publishes no "
                "output schema; call it with call_untyped() instead",
            )
        structured = await self._call(tool, arguments)
        if structured is None:
            raise AgentRunError(
                AgentRunErrorCode.TOOL_RESULT_UNSTRUCTURED,
                f"tool {tool!r} of mcp server {self._capability.server!r} published an "
                "output schema but returned no structured content",
            )
        try:
            return msgspec.convert(structured, expect)
        except msgspec.ValidationError as exc:
            raise AgentRunError(
                AgentRunErrorCode.TOOL_DECODE_FAILED,
                f"tool {tool!r} of mcp server {self._capability.server!r}: {exc}",
            ) from exc

    async def call_untyped(self, tool: str, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        """Call *tool* and return the server's own structured content, undecoded."""
        self._require_tool(tool)
        structured = await self._call(tool, arguments)
        return structured if isinstance(structured, Mapping) else {}

    async def _call(self, tool: str, arguments: Mapping[str, Any]) -> object | None:
        async with self._guard(tool):
            result = await self._session.call_tool(tool, arguments)
        if not result.ok:
            raise AgentRunError(
                AgentRunErrorCode.TOOL_CALL_FAILED,
                f"tool {tool!r} of mcp server {self._capability.server!r} reported a failure",
            )
        return result.structured

    def _require_tool(self, tool: str) -> McpToolInfo:
        info = self._tools.get(tool)
        if info is None:
            granted = ", ".join(sorted(self._tools)) or "none"
            raise AgentRunError(
                AgentRunErrorCode.TOOL_UNKNOWN,
                f"mcp server {self._capability.server!r} grants no tool named {tool!r}; "
                f"tools this grant admits: {granted}",
            )
        return info

    @asynccontextmanager
    async def _guard(self, tool: str) -> AsyncIterator[None]:
        """Bound one call by the plan's tool timeout, inside its own span."""
        with self._span(tool):
            try:
                async with asyncio.timeout(self._timeout_s):
                    yield
            except TimeoutError as exc:
                raise AgentRunError(
                    AgentRunErrorCode.TOOL_TIMEOUT,
                    f"tool {tool!r} exceeded the {self._timeout_s:.3f}s tool timeout",
                ) from exc

    def _span(self, tool: str) -> AbstractContextManager[None]:
        if self._observability is None:
            return nullcontext()
        return self._observability.span(
            Scope.TOOL,
            tool,
            agent=self._agent,
            capability="mcp",
            subject=self._identity.subject,
        )


class SqlGrantView:
    """The concrete :class:`~loom.ai.abc.SqlGrantHandle` an ``AgentHandle.sql()`` returns."""

    def __init__(
        self,
        *,
        agent: str,
        capability: CompiledSqlCapability,
        container: LoomContainer,
        identity: Identity,
        observability: ObservabilityRuntime | None,
    ) -> None:
        self._agent = agent
        self._capability = capability
        self._container = container
        self._identity = identity
        self._observability = observability

    async def query(
        self,
        statement: str,
        *,
        parameters: Mapping[str, Any] | None = None,
    ) -> Sequence[Mapping[str, Any]]:
        """Run *statement* with the caller's roles, under this grant's own bounds."""
        roles = self._bound_roles()
        service: SqlQueryService = self._container.resolve(SqlQueryService)
        with self._span(roles):
            result = await service.execute(
                statement,
                connection=self._capability.connection,
                roles=roles,
                parameters=parameters,
                limit=self._capability.max_rows,
            )
        return _bounded_rows(self._capability, result)

    def _bound_roles(self) -> tuple[str, ...]:
        """Resolve the caller's roles; the connection's shared role is unreachable.

        Mirrors :func:`~loom.ai.engines.pydantic_ai._capabilities._bound_roles`
        exactly: ``roles_bound`` is hard-coded ``True`` and the result is
        re-checked for emptiness, because ``()`` would fall through
        ``SqlQueryService.execute`` to the connection's shared ``default_role``
        (FR-043a).
        """
        allowed = frozenset(self._capability.config.allowed_roles)
        try:
            roles = resolve_query_roles(
                self._identity,
                connection=self._capability.connection,
                roles_bound=True,
                allowed_roles=allowed,
                requested_roles=None,
            )
        except (RolesNotBoundError, RoleNotAllowedError) as exc:
            raise AgentRunError(
                AgentRunErrorCode.UNAUTHORIZED,
                f"the caller may not query the {self._capability.connection!r} connection",
            ) from exc
        if not roles:
            raise AgentRunError(
                AgentRunErrorCode.UNAUTHORIZED,
                "no role of the caller is allowlisted on the "
                f"{self._capability.connection!r} connection",
            )
        return roles

    def _span(self, roles: tuple[str, ...]) -> AbstractContextManager[None]:
        if self._observability is None:
            return nullcontext()
        return self._observability.span(
            Scope.READ,
            f"sql:{self._capability.connection}",
            agent=self._agent,
            connection=self._capability.connection,
            roles=",".join(roles),
            subject=self._identity.subject,
        )


def _bounded_rows(
    capability: CompiledSqlCapability, result: SqlQueryResult
) -> tuple[Mapping[str, Any], ...]:
    """Turn tabular rows into mappings, truncated to the grant's byte bound.

    The row bound is already enforced upstream, by passing
    ``limit=capability.max_rows`` into the query itself; this only trims
    trailing rows so the encoded payload never exceeds ``max_result_bytes`` —
    the same "truncate, don't refuse" contract
    :meth:`~loom.ai.abc.SqlGrantHandle.query` documents.
    """
    columns = tuple(column.name for column in result.columns)
    kept: list[Mapping[str, Any]] = []
    for row in result.rows:
        candidate = [*kept, dict(zip(columns, row, strict=True))]
        if len(msgspec.json.encode(candidate)) > capability.max_result_bytes:
            break
        kept.append(candidate[-1])
    return tuple(kept)


__all__ = ["McpGrantView", "SqlGrantView"]
