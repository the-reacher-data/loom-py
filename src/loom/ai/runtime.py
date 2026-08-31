"""Live agent runtime: one entered lifecycle, shared clients, bounded runs.

Mirrors :class:`~loom.core.sql.clickhouse.registry.ClickHouseConnectionRegistry`:
the runtime exists only between ``__aenter__`` and ``__aexit__``, so there is no
intermediate started/stopped state. Entering opens every live client the plans
declare — concurrently — through a single
:class:`~contextlib.AsyncExitStack` owned by the entering task, validates the
declared tool filters against the tools each server really exposes, re-verifies
the read-only state of every SQL grant against live configuration, and builds
one engine per plan. Connecting and validating share a single absolute
deadline, so ``startup_timeout_ms`` bounds the whole of start-up once, whatever
the number of servers. Leaving closes everything in strict reverse order.

Nothing here imports FastAPI or Starlette: the HTTP surface lives in
:mod:`loom.ai.fastapi` and this module stays usable from any transport.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Callable,
    Coroutine,
    Iterable,
    Mapping,
    Sequence,
)
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from types import MappingProxyType, TracebackType
from typing import Any, Protocol, Self, TypeVar
from urllib.parse import urlparse

from loom.ai.abc import (
    AgentEngine,
    AgentEngineProvider,
    AgentEvent,
    AgentResult,
    DepsFactory,
    ErrorEvent,
    FinalEvent,
    HealthState,
    ToolCallEvent,
    ToolResultEvent,
)
from loom.ai.compiler._plan import (
    AgentPlan,
    CompiledA2ACapability,
    CompiledMcpCapability,
    CompiledSqlCapability,
)
from loom.ai.config import AiConfig
from loom.ai.declarative import PolicySpec, ToolFilter
from loom.ai.errors import (
    AgentCompilationError,
    AgentCompilationIssue,
    AgentRunErrorCode,
    a2a_agent_unreachable,
    mcp_server_unreachable,
    sql_readonly_drift,
    tool_filter_matches_nothing,
)
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from loom.core.model import LoomFrozenStruct
from loom.core.sql.config import SqlConfig

_logger = logging.getLogger(__name__)

_T = TypeVar("_T")


"""Aggregate health vocabulary shared with the HTTP contract."""

# Worst-first ordering: the aggregate of several dependencies is the worst of
# them, so a single unavailable server is never hidden by healthy neighbours.
_STATE_ORDER: Mapping[str, int] = MappingProxyType({"ok": 0, "degraded": 1, "unavailable": 2})
_STATE_BY_RANK: Mapping[int, HealthState] = MappingProxyType(
    {0: "ok", 1: "degraded", 2: "unavailable"}
)

_EMPTY_CHECKS: Mapping[str, str] = MappingProxyType({})

_TERMINAL_EVENT_TYPES: frozenset[type] = frozenset({ErrorEvent, FinalEvent})


class McpSession(Protocol):
    """Minimal MCP session the runtime needs from any client library."""

    async def list_tools(self) -> tuple[str, ...]:
        """Return the tool names the server exposes."""
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

A2AClientFactory = Callable[[CompiledA2ACapability], AbstractAsyncContextManager[object]]
"""Builds the (not yet opened) client of one compiled A2A capability."""


class AgentRunError(Exception):
    """A run failed with a stable, machine-readable code.

    Args:
        code: Run-time failure code; the retry policy reads its class.
        message: Human-readable description, safe to return to the caller.

    Attributes:
        code: The failure code carried by this error.
    """

    def __init__(self, code: AgentRunErrorCode, message: str) -> None:
        super().__init__(message)
        self.code = code


class AgentHealth(LoomFrozenStruct, frozen=True, kw_only=True):
    """Cached health of one agent and of its live dependencies.

    Attributes:
        status: Aggregate state, the worst of every check.
        checks: Per-dependency state, keyed ``"model"``, ``"mcp:<host>"``,
            ``"a2a:<url>"`` or ``"sql:<connection>"``. Internal topology: only
            an authenticated caller ever sees it (FR-029c).
        detail: Optional explanation, ``"probing"`` until the first probe of
            the background refresher completes.
    """

    status: HealthState
    checks: Mapping[str, str] = _EMPTY_CHECKS
    detail: str | None = None


_PROBING = AgentHealth(status="degraded", detail="probing")


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
        """Return the tool names the server exposes, serialised with every other call."""
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


@dataclass(frozen=True, slots=True)
class _AgentSlot:
    """One compiled plan and the single engine built for it."""

    plan: AgentPlan
    engine: AgentEngine


@dataclass(frozen=True, slots=True)
class _OpenedClient:
    """One live client, in the order its connection actually completed."""

    key: str
    client: AbstractAsyncContextManager[Any]
    session: object


def _dependency_key(capability: object) -> str | None:
    """Return the health-check key of a capability with a live dependency."""
    if type(capability) is CompiledMcpCapability:
        return f"mcp:{urlparse(capability.url).hostname or capability.url}"
    if type(capability) is CompiledA2ACapability:
        return f"a2a:{capability.url}"
    if type(capability) is CompiledSqlCapability:
        return f"sql:{capability.connection}"
    return None


def _filtered_tools(tools: Sequence[str], tool_filter: ToolFilter) -> tuple[str, ...]:
    """Apply ``include`` then ``exclude`` to the tools a server offers."""
    include = frozenset(tool_filter.include)
    exclude = frozenset(tool_filter.exclude)
    included = tools if not include else [tool for tool in tools if tool in include]
    return tuple(tool for tool in included if tool not in exclude)


@dataclass(frozen=True, slots=True)
class _FilterTarget:
    """One declared tool filter and the shared session it must be checked against."""

    agent: str
    url: str
    key: str
    tool_filter: ToolFilter


def _filter_targets(plans: Iterable[AgentPlan]) -> tuple[_FilterTarget, ...]:
    """Return every declared MCP tool filter, in plan then declaration order."""
    return tuple(
        _FilterTarget(
            agent=plan.name,
            url=capability.url,
            key=_mcp_key(capability),
            tool_filter=capability.tool_filter,
        )
        for plan in plans
        for capability in plan.capabilities
        if type(capability) is CompiledMcpCapability and capability.tool_filter is not None
    )


def _filter_issues(
    targets: Iterable[_FilterTarget], listed: Mapping[str, tuple[str, ...]]
) -> list[AgentCompilationIssue]:
    """Return one issue per filter that selects none of its server's tools."""
    return [
        tool_filter_matches_nothing(target.agent, target.url)
        for target in targets
        if target.key in listed and not _filtered_tools(listed[target.key], target.tool_filter)
    ]


def _listing_timeout_issues(
    targets: Iterable[_FilterTarget], listed: Mapping[str, tuple[str, ...]]
) -> list[AgentCompilationIssue]:
    """Name every server whose tool listing did not complete inside the budget."""
    pending: dict[str, str] = {
        target.key: target.url for target in targets if target.key not in listed
    }
    return [mcp_server_unreachable(url, "listing its tools timed out") for url in pending.values()]


def _worst(states: Iterable[str]) -> HealthState:
    """Return the worst of several dependency states, ``"ok"`` when there are none."""
    ranks = (_STATE_ORDER.get(state, 2) for state in states)
    return _STATE_BY_RANK[max(ranks, default=0)]


class _RunSupervisor:
    """Enforces the per-run limits of one plan over its event stream.

    Keeps the whole limit vocabulary in one place so ``/run`` and ``/stream``
    share a single code path: a breach becomes the stream's terminal
    :class:`~loom.ai.abc.ErrorEvent`, which ``run()`` turns back into an
    :class:`AgentRunError`.

    Args:
        policies: Validated execution limits of the plan being run.
    """

    def __init__(self, policies: PolicySpec) -> None:
        loop = asyncio.get_running_loop()
        self._now = loop.time
        self._run_deadline = loop.time() + policies.run_timeout_ms / 1000
        self._policies = policies
        self._tool_deadline: float | None = None
        self._pending_tool: str | None = None
        self._pending_call: str | None = None
        self._iterations = 0
        self.terminated = False

    @property
    def deadline(self) -> float:
        """Absolute loop time the next event must arrive before."""
        if self._tool_deadline is None:
            return self._run_deadline
        return min(self._run_deadline, self._tool_deadline)

    def timeout_event(self) -> ErrorEvent:
        """Return the terminal event of an expired deadline, naming what expired."""
        tool_expired = (
            self._pending_tool is not None
            and self._tool_deadline is not None
            and self._tool_deadline <= self._run_deadline
        )
        if tool_expired:
            return ErrorEvent(
                code=AgentRunErrorCode.TOOL_TIMEOUT,
                message=(
                    f"tool {self._pending_tool!r} exceeded tool_timeout_ms "
                    f"({self._policies.tool_timeout_ms})"
                ),
            )
        return ErrorEvent(
            code=AgentRunErrorCode.RUN_TIMEOUT,
            message=f"run exceeded run_timeout_ms ({self._policies.run_timeout_ms})",
        )

    def observe(self, event: AgentEvent) -> ErrorEvent | None:
        """Account for one event and return the terminal event of a breach.

        Args:
            event: Event just produced by the engine.

        Returns:
            The terminal :class:`~loom.ai.abc.ErrorEvent` when a limit is
            breached, ``None`` when the event may be forwarded.
        """
        if type(event) is ToolCallEvent:
            return self._observe_tool_call(event)
        if type(event) is ToolResultEvent:
            self._observe_tool_result(event)
            return None
        self.terminated = type(event) in _TERMINAL_EVENT_TYPES
        return None

    def _observe_tool_call(self, event: ToolCallEvent) -> ErrorEvent | None:
        self._iterations += 1
        if self._iterations > self._policies.max_iterations:
            return ErrorEvent(
                code=AgentRunErrorCode.MAX_ITERATIONS_EXCEEDED,
                message=f"run exceeded max_iterations ({self._policies.max_iterations})",
            )
        self._pending_tool = event.tool
        self._pending_call = event.call_id
        self._tool_deadline = self._now() + self._policies.tool_timeout_ms / 1000
        return None

    def _observe_tool_result(self, event: ToolResultEvent) -> None:
        if event.call_id != self._pending_call:
            return
        self._pending_tool = None
        self._pending_call = None
        self._tool_deadline = None


async def _supervised_events(
    events: AsyncIterator[AgentEvent], policies: PolicySpec
) -> AsyncGenerator[AgentEvent, None]:
    """Forward an engine stream while enforcing the plan's per-run limits."""
    supervisor = _RunSupervisor(policies)
    iterator = events.__aiter__()
    while True:
        try:
            async with asyncio.timeout_at(supervisor.deadline):
                event = await anext(iterator)
        except StopAsyncIteration:
            return
        except TimeoutError:
            yield supervisor.timeout_event()
            return
        breach = supervisor.observe(event)
        if breach is not None:
            yield breach
            return
        yield event
        if supervisor.terminated:
            return


async def _cancel_task(task: asyncio.Task[None]) -> None:
    """Cancel an owned background task and wait for it to actually stop."""
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        return


class AgentRuntime:
    """Owns the live agents of one worker: clients, engines and their limits.

    Usable only as an async context manager, and only from the task that
    entered it: every live client is opened through one
    :class:`~contextlib.AsyncExitStack` created in ``__aenter__``, so closing
    happens in strict reverse order in the same task. That is what keeps a
    session-affine client (MCP over a framed transport) from being closed by a
    task that never opened it.

    Args:
        plans: Compiled plans this worker serves.
        config: Deployment configuration of the AI pillar.
        engine_provider: Provider building one engine per plan, exactly once.
        deps: Per-invocation dependency factory handed to every engine.
        container: Application container the engines resolve services from.
        sql_config: Live ``sql:`` configuration, re-verified at start-up
            against what the plans were compiled against (FR-046).
        mcp_client_factory: Builds the client of one MCP capability. Required
            when any plan declares an ``mcp`` capability: without it the
            declared tool filters cannot be validated, so start-up fails
            closed rather than serving unvalidated grants.
        a2a_client_factory: Builds the client of one A2A capability, with the
            same fail-closed rule.

    Runs emit no span of their own: the transport owns observability, because
    only it knows the route, the method and the status code a run is attributed
    to. Over HTTP that owner is
    :func:`~loom.ai.fastapi.endpoints.bind_agent_endpoints`.

    Raises:
        AgentCompilationError: From ``__aenter__``, aggregating every start-up
            failure — an unreachable server (naming its URL), a tool filter
            matching nothing, or a SQL connection whose read-only state drifted.

    Example::

        async with AgentRuntime(
            plans=plans, config=ai_config, engine_provider=provider,
            deps=deps, container=container,
        ) as runtime:
            result = await runtime.run("analyst", prompt, identity=identity)
    """

    def __init__(
        self,
        *,
        plans: Sequence[AgentPlan],
        config: AiConfig,
        engine_provider: AgentEngineProvider,
        deps: DepsFactory,
        container: LoomContainer,
        sql_config: SqlConfig | None = None,
        mcp_client_factory: McpClientFactory | None = None,
        a2a_client_factory: A2AClientFactory | None = None,
    ) -> None:
        self._plans: Mapping[str, AgentPlan] = MappingProxyType({p.name: p for p in plans})
        self._config = config
        self._engine_provider = engine_provider
        self._deps = deps
        self._container = container
        self._sql_config = sql_config
        self._mcp_client_factory = mcp_client_factory
        self._a2a_client_factory = a2a_client_factory
        self._stack: AsyncExitStack | None = None
        self._owner: asyncio.Task[Any] | None = None
        self._slots: dict[str, _AgentSlot] = {}
        self._sessions: dict[str, SharedMcpSession] = {}
        self._live: set[str] = set()
        self._health: dict[str, AgentHealth] = {}
        self._runs = asyncio.Semaphore(config.max_concurrent_runs)

    async def __aenter__(self) -> Self:
        """Open every live client, validate the plans and build the engines.

        Returns:
            The entered runtime.

        Raises:
            RuntimeError: When the runtime was already entered.
            AgentCompilationError: Aggregating every start-up failure. Every
                client opened so far is closed before the error propagates.
        """
        if self._stack is not None:
            raise RuntimeError("AgentRuntime is already entered")
        stack = AsyncExitStack()
        self._stack = stack
        self._owner = asyncio.current_task()
        deadline = asyncio.get_running_loop().time() + self._config.startup_timeout_ms / 1000
        try:
            self._verify_sql_readonly()
            await self._open_clients(stack, deadline)
            await self._verify_tool_filters(deadline)
            self._build_engines()
            self._start_health_probe(stack)
        except BaseException:
            self._stack = None
            self._owner = None
            await stack.aclose()
            raise
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close every live client in reverse order, in the task that opened them.

        Raises:
            RuntimeError: When the runtime was never entered, or when the
                exiting task is not the one that entered it — closing a
                session-affine client from a foreign task is a latent
                corruption, not a detail to paper over.
        """
        stack = self._stack
        if stack is None:
            raise RuntimeError("AgentRuntime was not entered")
        if asyncio.current_task() is not self._owner:
            raise RuntimeError(
                "AgentRuntime must be exited by the task that entered it: the live "
                "clients are session-affine and closing them from another task "
                "corrupts them"
            )
        self._stack = None
        self._owner = None
        self._slots.clear()
        self._sessions.clear()
        self._live.clear()
        await stack.aclose()

    def agent_names(self) -> tuple[str, ...]:
        """Return the names of every agent this runtime serves."""
        return tuple(self._plans)

    def has_agent(self, name: str) -> bool:
        """Report whether an agent with that name is served by this runtime.

        Args:
            name: Agent name to look for.

        Returns:
            ``True`` when the runtime holds a plan with that name.
        """
        return name in self._plans

    def capability_kinds(self, name: str) -> tuple[str, ...]:
        """Return the capability kinds one agent was granted.

        Lets a transport state what a route exposes without reaching into the
        compiled plan, which carries resolved handles and instructions.

        Args:
            name: Agent to describe.

        Returns:
            The distinct ``kind`` identifiers of the agent's capabilities, in
            declaration order.

        Raises:
            KeyError: When no agent is named *name*.
        """
        plan = self._require_plan(name)
        kinds = {capability.kind: None for capability in plan.capabilities}
        return tuple(kinds)

    async def run(self, name: str, prompt: str, *, identity: Identity) -> AgentResult:
        """Run one agent to completion.

        Args:
            name: Agent to run.
            prompt: Caller prompt.
            identity: Verified caller; every capability call runs as them.

        Returns:
            The decoded output and the run's usage.

        Raises:
            KeyError: When no agent is named *name*.
            AgentRunError: When the run is refused (``TOO_MANY_RUNS``), breaches
                a declared limit, or ends in a failure event.
        """
        result: AgentResult | None = None
        async with self._run_stream(name, prompt, identity=identity) as events:
            async for event in events:
                if type(event) is ErrorEvent:
                    raise AgentRunError(event.code, str(event.message))
                if type(event) is FinalEvent:
                    result = AgentResult(output=event.output, usage=event.usage)
        if result is None:
            raise AgentRunError(
                AgentRunErrorCode.PROVIDER_UNAVAILABLE,
                f"agent {name!r} produced no terminal event",
            )
        return result

    def run_stream(
        self, name: str, prompt: str, *, identity: Identity
    ) -> AbstractAsyncContextManager[AsyncIterator[AgentEvent]]:
        """Run one agent, streaming its supervised events.

        The stream is an async context manager so the engine connection behind
        it closes deterministically instead of waiting for the collector.

        Args:
            name: Agent to run.
            prompt: Caller prompt.
            identity: Verified caller; every capability call runs as them.

        Returns:
            An async context manager yielding the limit-supervised events.

        Raises:
            KeyError: When no agent is named *name*.
            AgentRunError: On entry, when the worker's ``max_concurrent_runs``
                is already taken (``TOO_MANY_RUNS``).
        """
        return self._run_stream(name, prompt, identity=identity)

    async def health(self, name: str) -> AgentHealth:
        """Return the cached health of one agent — never network I/O per call.

        Args:
            name: Agent to report on.

        Returns:
            The last state the background probe recorded, or a ``degraded``
            health with ``detail="probing"`` before its first pass completes.

        Raises:
            KeyError: When no agent is named *name*.
        """
        self._require_plan(name)
        return self._health.get(name, _PROBING)

    # -- lifecycle ---------------------------------------------------------

    def _verify_sql_readonly(self) -> None:
        """Abort start-up when a SQL grant's read-only state drifted (FR-046)."""
        issues = [
            sql_readonly_drift(capability.connection)
            for plan in self._plans.values()
            for capability in plan.capabilities
            if type(capability) is CompiledSqlCapability and self._sql_drifted(capability)
        ]
        if issues:
            raise AgentCompilationError(issues)

    def _sql_drifted(self, capability: CompiledSqlCapability) -> bool:
        live = (
            None
            if self._sql_config is None
            else self._sql_config.connections.get(capability.connection)
        )
        return live is None or live.readonly != capability.config.readonly

    async def _open_clients(self, stack: AsyncExitStack, deadline: float) -> None:
        """Open every live client concurrently, before the start-up deadline."""
        mcp, a2a = _url_capabilities(self._plans.values())
        if not mcp and not a2a:
            return
        opened: list[_OpenedClient] = []
        failures: list[AgentCompilationIssue] = []
        try:
            async with asyncio.timeout_at(deadline):
                async with asyncio.TaskGroup() as group:
                    for capability in mcp:
                        group.create_task(self._open_mcp(capability, opened, failures))
                    for remote in a2a:
                        group.create_task(self._open_a2a(remote, opened, failures))
        except TimeoutError:
            failures.extend(self._timeout_issues(mcp, a2a, opened))
        finally:
            # Registered from the entering task, in completion order, so the
            # exit stack unwinds them in strict reverse order.
            self._register_opened(stack, opened)
        if failures:
            raise AgentCompilationError(failures)

    async def _open_mcp(
        self,
        capability: CompiledMcpCapability,
        opened: list[_OpenedClient],
        failures: list[AgentCompilationIssue],
    ) -> None:
        factory = self._mcp_client_factory
        if factory is None:
            failures.append(
                mcp_server_unreachable(capability.url, "no MCP client factory is configured")
            )
            return
        client = factory(capability)
        try:
            session = await client.__aenter__()
        except Exception as exc:  # recovery: reported as a coded start-up issue
            failures.append(mcp_server_unreachable(capability.url, str(exc)))
            return
        opened.append(_OpenedClient(key=_mcp_key(capability), client=client, session=session))

    async def _open_a2a(
        self,
        capability: CompiledA2ACapability,
        opened: list[_OpenedClient],
        failures: list[AgentCompilationIssue],
    ) -> None:
        factory = self._a2a_client_factory
        if factory is None:
            failures.append(
                a2a_agent_unreachable(capability.url, "no A2A client factory is configured")
            )
            return
        client = factory(capability)
        try:
            session = await client.__aenter__()
        except Exception as exc:  # recovery: reported as a coded start-up issue
            failures.append(a2a_agent_unreachable(capability.url, str(exc)))
            return
        opened.append(_OpenedClient(key=f"a2a:{capability.url}", client=client, session=session))

    def _register_opened(self, stack: AsyncExitStack, opened: Sequence[_OpenedClient]) -> None:
        for entry in opened:
            stack.push_async_exit(entry.client)
            self._live.add(entry.key)
            if entry.key.startswith("mcp:"):
                session: McpSession = entry.session  # type: ignore[assignment]
                self._sessions[entry.key] = SharedMcpSession(session, label=entry.key)

    def _timeout_issues(
        self,
        mcp: Sequence[CompiledMcpCapability],
        a2a: Sequence[CompiledA2ACapability],
        opened: Sequence[_OpenedClient],
    ) -> list[AgentCompilationIssue]:
        """Name every server whose connection did not complete in the budget."""
        reason = f"connection did not complete within {self._config.startup_timeout_ms} ms"
        live = {entry.key for entry in opened}
        issues: list[AgentCompilationIssue] = [
            mcp_server_unreachable(capability.url, reason)
            for capability in mcp
            if _mcp_key(capability) not in live
        ]
        issues.extend(
            a2a_agent_unreachable(remote.url, reason)
            for remote in a2a
            if f"a2a:{remote.url}" not in live
        )
        return issues

    async def _verify_tool_filters(self, deadline: float) -> None:
        """Apply every declared tool filter to the tools really offered (FR-025).

        Tools are listed once per shared session, never once per (plan,
        capability) pair: sessions are shared per server, so two plans pointing
        at the same server would otherwise pay two serialised round trips for
        identical data.
        """
        targets = _filter_targets(self._plans.values())
        if not targets:
            return
        listed: dict[str, tuple[str, ...]] = {}
        try:
            async with asyncio.timeout_at(deadline):
                await self._list_tools_once(targets, listed)
        except TimeoutError:
            raise AgentCompilationError(_listing_timeout_issues(targets, listed)) from None
        issues = _filter_issues(targets, listed)
        if issues:
            raise AgentCompilationError(issues)

    async def _list_tools_once(
        self, targets: Sequence[_FilterTarget], listed: dict[str, tuple[str, ...]]
    ) -> None:
        """List the tools of every session a declared filter applies to, once per session."""
        for target in targets:
            session = self._sessions.get(target.key)
            if session is None or target.key in listed:
                continue
            listed[target.key] = await session.list_tools()

    def _build_engines(self) -> None:
        """Build one engine per plan, exactly once per worker (FR-026)."""
        for name, plan in self._plans.items():
            engine = self._engine_provider.create_engine(
                plan, deps=self._deps, container=self._container
            )
            self._slots[name] = _AgentSlot(plan=plan, engine=engine)

    def _start_health_probe(self, stack: AsyncExitStack) -> None:
        """Start the single owned probe refreshing the declared health cache."""
        probe = asyncio.create_task(self._probe_forever(), name="loom-agent-health-probe")
        stack.push_async_callback(_cancel_task, probe)

    async def _probe_forever(self) -> None:
        period = max(self._config.health_cache_ttl_ms, 1) / 1000
        while True:
            for name in tuple(self._slots):
                self._health[name] = await self._probe(name)
            await asyncio.sleep(period)

    async def _probe(self, name: str) -> AgentHealth:
        slot = self._slots[name]
        engine_status = await slot.engine.health()
        checks: dict[str, str] = {"model": engine_status.status}
        for capability in slot.plan.capabilities:
            key = _dependency_key(capability)
            if key is not None:
                checks[key] = self._dependency_state(key)
        return AgentHealth(status=_worst(checks.values()), checks=MappingProxyType(checks))

    def _dependency_state(self, key: str) -> HealthState:
        if key.startswith("sql:"):
            return "ok"
        return "ok" if key in self._live else "unavailable"

    # -- runs --------------------------------------------------------------

    @asynccontextmanager
    async def _run_stream(
        self, name: str, prompt: str, *, identity: Identity
    ) -> AsyncIterator[AsyncIterator[AgentEvent]]:
        slot = self._require_slot(name)
        await self._admit(name)
        try:
            async with slot.engine.run_stream(prompt, identity=identity) as events:
                supervised = _supervised_events(events, slot.plan.policies)
                try:
                    yield supervised
                finally:
                    await supervised.aclose()
        finally:
            self._runs.release()

    async def _admit(self, name: str) -> None:
        """Take a run slot, refusing instead of queueing when none is free."""
        if self._runs.locked():
            raise AgentRunError(
                AgentRunErrorCode.TOO_MANY_RUNS,
                (
                    f"agent {name!r}: this worker already serves its "
                    f"max_concurrent_runs ({self._config.max_concurrent_runs})"
                ),
            )
        # Never suspends: the guard above proved a permit is available.
        await self._runs.acquire()

    def _require_plan(self, name: str) -> AgentPlan:
        plan = self._plans.get(name)
        if plan is None:
            raise KeyError(name)
        return plan

    def _require_slot(self, name: str) -> _AgentSlot:
        self._require_plan(name)
        slot = self._slots.get(name)
        if slot is None:
            raise RuntimeError(
                "AgentRuntime must be entered before use: wrap it in "
                "'async with runtime:' to open its clients and build its engines"
            )
        return slot


def _mcp_key(capability: CompiledMcpCapability) -> str:
    """Return the health-check key of one MCP capability."""
    return f"mcp:{urlparse(capability.url).hostname or capability.url}"


def _url_capabilities(
    plans: Iterable[AgentPlan],
) -> tuple[tuple[CompiledMcpCapability, ...], tuple[CompiledA2ACapability, ...]]:
    """Return one MCP and one A2A capability per distinct URL across every plan.

    Clients are shared per worker, not per agent and never per call (FR-026),
    so two agents pointing at the same server open a single connection.

    Args:
        plans: Compiled plans of this worker.

    Returns:
        The de-duplicated MCP capabilities and A2A capabilities.
    """
    mcp: dict[str, CompiledMcpCapability] = {}
    a2a: dict[str, CompiledA2ACapability] = {}
    for plan in plans:
        for capability in plan.capabilities:
            if type(capability) is CompiledMcpCapability:
                mcp.setdefault(capability.url, capability)
            elif type(capability) is CompiledA2ACapability:
                a2a.setdefault(capability.url, capability)
    return tuple(mcp.values()), tuple(a2a.values())
