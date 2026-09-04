"""One entered runtime lifecycle: shared clients opened once, engines built once.

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
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable, Iterable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from types import MappingProxyType, TracebackType
from typing import Any, Self
from uuid import uuid4

from loom.ai.abc import (
    CONVERSATION_ID_MAX_LENGTH,
    AgentEngine,
    AgentEngineProvider,
    AgentEvent,
    AgentResult,
    DepsFactory,
    ErrorEvent,
    FinalEvent,
    HealthState,
)
from loom.ai.compiler._plan import (
    AgentPlan,
    CompiledA2ACapability,
    CompiledMcpCapability,
    CompiledSqlCapability,
)
from loom.ai.config import AiConfig
from loom.ai.errors import (
    AgentCompilationError,
    AgentCompilationIssue,
    AgentRunError,
    AgentRunErrorCode,
    a2a_agent_unreachable,
    mcp_server_unreachable,
    on_output_invoker_missing,
    sql_readonly_drift,
)
from loom.ai.runtime._health import AgentHealth, worst
from loom.ai.runtime._hooks import HookRun, hooked_events, no_terminal_message
from loom.ai.runtime._limits import cancel_task, supervised_events
from loom.ai.runtime._mcp import (
    FilterTarget,
    McpClientFactory,
    McpSession,
    SharedMcpSession,
    filter_issues,
    filter_targets,
    listing_timeout_issues,
    mcp_key,
)
from loom.core.di import LoomContainer
from loom.core.identity import ANONYMOUS, Identity
from loom.core.sql.config import SqlConfig
from loom.core.use_case.invoker import ApplicationInvoker

_logger = logging.getLogger(__name__)


A2AClientFactory = Callable[[CompiledA2ACapability], AbstractAsyncContextManager[object]]
"""Builds the (not yet opened) client of one compiled A2A capability."""

_PROBING = AgentHealth(status="degraded", detail="probing")

_PROBE_FAILED = "the health probe failed; the detail is recorded server-side"
"""Detail of an agent whose engine probe raised. No exception text: the probe
reaches a model provider, so its failures carry endpoints and credential
references that an anonymous ``/health`` scrape must never receive."""

_INVOKER_UNBOUND = "the use-case invoker is not bound to a caller"


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
        return mcp_key(capability)
    if type(capability) is CompiledA2ACapability:
        return _a2a_key(capability)
    if type(capability) is CompiledSqlCapability:
        return f"sql:{capability.connection}"
    return None


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
            failure — an unreachable server (named as the deployment
            registered it, never by URL), a tool filter
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
            self._verify_hook_invoker()
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
        """Return the names of every agent this runtime serves.

        Returns:
            One name per compiled plan, in the order the plans were given.
        """
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

    async def run(
        self,
        name: str,
        prompt: str,
        *,
        identity: Identity,
        conversation_id: str | None = None,
    ) -> AgentResult:
        """Run one agent to completion.

        Args:
            name: Agent to run.
            prompt: Caller prompt.
            identity: Verified caller; every capability call runs as them.
            conversation_id: Opaque value the application supplies; copied
                verbatim into the output hook's command, never read.

        Returns:
            The decoded output, the run's usage, its ``interaction_id`` and the
            output hook's result.

        Raises:
            KeyError: When no agent is named *name*.
            ValueError: When ``conversation_id`` is empty or longer than
                :data:`~loom.ai.abc.CONVERSATION_ID_MAX_LENGTH`.
            AgentRunError: When the run is refused (``TOO_MANY_RUNS``), breaches
                a declared limit, or ends in a failure event.
        """
        result: AgentResult | None = None
        stream = self._run_stream(name, prompt, identity=identity, conversation_id=conversation_id)
        async with stream as events:
            async for event in events:
                if type(event) is ErrorEvent:
                    raise AgentRunError(
                        event.code, str(event.message), interaction_id=event.interaction_id
                    )
                if type(event) is FinalEvent:
                    result = AgentResult(
                        output=event.output,
                        usage=event.usage,
                        interaction_id=event.interaction_id,
                        hook_result=event.hook_result,
                    )
        if result is None:
            # Defensive only: ``hooked_events`` closes every exhausted stream
            # with a named error, so this guard cannot be reached today.
            raise AgentRunError(AgentRunErrorCode.PROVIDER_UNAVAILABLE, no_terminal_message(name))
        return result

    def run_stream(
        self,
        name: str,
        prompt: str,
        *,
        identity: Identity,
        conversation_id: str | None = None,
    ) -> AbstractAsyncContextManager[AsyncIterator[AgentEvent]]:
        """Run one agent, streaming its supervised events.

        The stream is an async context manager so the engine connection behind
        it closes deterministically instead of waiting for the collector.

        Args:
            name: Agent to run.
            prompt: Caller prompt.
            identity: Verified caller; every capability call runs as them.
            conversation_id: Opaque value the application supplies; copied
                verbatim into the output hook's command, never read.

        Returns:
            An async context manager yielding the limit-supervised events; the
            terminal event carries the run's ``interaction_id``.

        Raises:
            KeyError: When no agent is named *name*.
            ValueError: On entry, when ``conversation_id`` is empty or longer
                than :data:`~loom.ai.abc.CONVERSATION_ID_MAX_LENGTH`.
            AgentRunError: On entry, when the worker's ``max_concurrent_runs``
                is already taken (``TOO_MANY_RUNS``).
        """
        return self._run_stream(name, prompt, identity=identity, conversation_id=conversation_id)

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

    def _verify_hook_invoker(self) -> None:
        """Abort start-up when a hook is declared but no bundle carries an invoker.

        Probed once, before any client opens: without it a misconfigured
        deployment would fail only after every paid run.
        """
        hooked = [name for name, plan in self._plans.items() if plan.on_output is not None]
        if not hooked:
            return
        invoker = getattr(self._deps.build(ANONYMOUS, self._container), "invoker", None)
        if not isinstance(invoker, ApplicationInvoker):
            raise AgentCompilationError([on_output_invoker_missing(hooked)])
        # An invoker built for a caller carries that caller (``ANONYMOUS`` here);
        # one carrying ``None`` was never bound and would run every hook as nobody.
        if getattr(invoker, "identity", ANONYMOUS) is None:
            raise AgentCompilationError(
                [on_output_invoker_missing(hooked, reason=_INVOKER_UNBOUND)]
            )

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
        mcp, a2a = _remote_capabilities(self._plans.values())
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
                mcp_server_unreachable(capability.server, "no MCP client factory is configured")
            )
            return
        client = factory(capability)
        try:
            session = await client.__aenter__()
        except Exception as exc:  # recovery: reported as a coded start-up issue
            failures.append(mcp_server_unreachable(capability.server, str(exc)))
            return
        opened.append(_OpenedClient(key=mcp_key(capability), client=client, session=session))

    async def _open_a2a(
        self,
        capability: CompiledA2ACapability,
        opened: list[_OpenedClient],
        failures: list[AgentCompilationIssue],
    ) -> None:
        factory = self._a2a_client_factory
        if factory is None:
            failures.append(
                a2a_agent_unreachable(capability.agent, "no A2A client factory is configured")
            )
            return
        client = factory(capability)
        try:
            session = await client.__aenter__()
        except Exception as exc:  # recovery: reported as a coded start-up issue
            failures.append(a2a_agent_unreachable(capability.agent, str(exc)))
            return
        opened.append(_OpenedClient(key=_a2a_key(capability), client=client, session=session))

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
            mcp_server_unreachable(capability.server, reason)
            for capability in mcp
            if mcp_key(capability) not in live
        ]
        issues.extend(
            a2a_agent_unreachable(remote.agent, reason)
            for remote in a2a
            if _a2a_key(remote) not in live
        )
        return issues

    async def _verify_tool_filters(self, deadline: float) -> None:
        """Apply every declared tool filter to the tools really offered (FR-025).

        Tools are listed once per shared session, never once per (plan,
        capability) pair: sessions are shared per server, so two plans pointing
        at the same server would otherwise pay two serialised round trips for
        identical data.
        """
        targets = filter_targets(self._plans.values())
        if not targets:
            return
        listed: dict[str, tuple[str, ...]] = {}
        try:
            async with asyncio.timeout_at(deadline):
                await self._list_tools_once(targets, listed)
        except TimeoutError:
            raise AgentCompilationError(listing_timeout_issues(targets, listed)) from None
        issues = filter_issues(targets, listed)
        if issues:
            raise AgentCompilationError(issues)

    async def _list_tools_once(
        self, targets: Sequence[FilterTarget], listed: dict[str, tuple[str, ...]]
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
        stack.push_async_callback(cancel_task, probe)

    async def _probe_forever(self) -> None:
        """Refresh the health cache forever; only cancellation ends this task.

        :meth:`~loom.ai.abc.AgentEngine.health` is a public protocol a third
        party implements, so it may raise anything. An escaping failure would
        end this task for good while ``/health`` kept answering the last cached
        ``ok`` — a dead probe reported as a healthy runtime. Instead the failing
        agent is recorded as ``unavailable`` and the loop moves on to the next.
        """
        period = max(self._config.health_cache_ttl_ms, 1) / 1000
        while True:
            for name in tuple(self._slots):
                self._health[name] = await self._probe_or_unavailable(name)
            await asyncio.sleep(period)

    async def _probe_or_unavailable(self, name: str) -> AgentHealth:
        """Probe one agent, reporting a failing probe as ``unavailable``."""
        try:
            return await self._probe(name)
        except Exception:  # recovery: a failed probe is a health state, not a crash
            _logger.exception("Health probe of agent %r failed", name)
            return AgentHealth(status="unavailable", detail=_PROBE_FAILED)

    async def _probe(self, name: str) -> AgentHealth:
        slot = self._slots[name]
        engine_status = await slot.engine.health()
        checks: dict[str, str] = {"model": engine_status.status}
        for capability in slot.plan.capabilities:
            key = _dependency_key(capability)
            if key is not None:
                checks[key] = self._dependency_state(key)
        return AgentHealth(status=worst(checks.values()), checks=MappingProxyType(checks))

    def _dependency_state(self, key: str) -> HealthState:
        if key.startswith("sql:"):
            return "ok"
        return "ok" if key in self._live else "unavailable"

    # -- runs --------------------------------------------------------------

    @asynccontextmanager
    async def _run_stream(
        self,
        name: str,
        prompt: str,
        *,
        identity: Identity,
        conversation_id: str | None,
    ) -> AsyncIterator[AsyncIterator[AgentEvent]]:
        slot = self._require_slot(name)
        _check_conversation_id(conversation_id)
        await self._admit(name)
        try:
            run = HookRun(
                plan=slot.plan,
                identity=identity,
                interaction_id=uuid4().hex,
                conversation_id=conversation_id,
            )
            async with slot.engine.run_stream(prompt, identity=identity) as events:
                supervised = supervised_events(events, slot.plan.policies)
                hooked = hooked_events(supervised, run, self._deps, self._container)
                try:
                    yield hooked
                finally:
                    await hooked.aclose()
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


def _check_conversation_id(conversation_id: str | None) -> None:
    """Refuse an out-of-bound ``conversation_id``: a programming error, not a run failure."""
    if conversation_id is None:
        return
    if not 1 <= len(conversation_id) <= CONVERSATION_ID_MAX_LENGTH:
        raise ValueError(
            f"conversation_id must be between 1 and {CONVERSATION_ID_MAX_LENGTH} "
            f"characters long, got {len(conversation_id)}"
        )


def _a2a_key(capability: CompiledA2ACapability) -> str:
    """Return the health-check key of one A2A capability, by registered name."""
    return f"a2a:{capability.agent}"


def _remote_capabilities(
    plans: Iterable[AgentPlan],
) -> tuple[tuple[CompiledMcpCapability, ...], tuple[CompiledA2ACapability, ...]]:
    """Return one MCP and one A2A capability per registered name across every plan.

    Clients are shared per worker, not per agent and never per call (FR-026),
    so two agents naming the same server open a single connection. The name is
    the unit of sharing, not the URL: two entries of ``ai.mcp_servers`` may
    legitimately share a host while differing in credential reference or
    deadline.

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
                mcp.setdefault(capability.server, capability)
            elif type(capability) is CompiledA2ACapability:
                a2a.setdefault(capability.agent, capability)
    return tuple(mcp.values()), tuple(a2a.values())
