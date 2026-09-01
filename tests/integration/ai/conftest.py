"""Fakes and builders shared by the Phase 5 (US3) agent runtime tests.

Nothing here touches the network, a credential or a token: every live client
is a local stub and every engine replays a fixed script.  The module
deliberately imports no symbol from ``loom.ai.runtime`` or
``loom.ai.fastapi``: a ``conftest`` import error aborts the whole session,
while the red state of the phase must come from each test module's own
import.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from types import TracebackType
from typing import Any

import msgspec
import pytest

from loom.ai.abc import (
    AgentEvent,
    AgentResult,
    AgentUsage,
    ErrorEvent,
    FinalEvent,
    HealthStatus,
    TextDeltaEvent,
)
from loom.ai.compiler._plan import (
    AgentPlan,
    CompiledCapability,
    CompiledMcpCapability,
    CompiledOutput,
    CompiledSqlCapability,
)
from loom.ai.config import A2AConfig, AgentEndpointConfig, AiConfig
from loom.ai.declarative import PolicySpec
from loom.ai.errors import AgentRunErrorCode
from loom.ai.inference import InferenceTarget
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from loom.core.sql.config import SqlConfig, SqlConnectionConfig

DEFAULT_USAGE = AgentUsage(input_tokens=11, output_tokens=7, requests=1, duration_ms=3)
"""Fixed usage every scripted terminal event carries."""

DEFAULT_OUTPUT: Mapping[str, Any] = {"answer": "42"}
"""Fixed decoded output the default script returns."""


# ---------------------------------------------------------------------------
# MCP stubs
# ---------------------------------------------------------------------------


class RecordingMcpSession:
    """MCP session double that records every call it serves.

    Args:
        label: Name used in the shared lifecycle log.
        tools: Tool names the server claims to expose.
        results: Result returned per tool name; missing names return ``None``.
        list_delay_ms: Time one ``list_tools`` round trip costs, so a test can
            express a start-up budget spent on listing rather than connecting.
    """

    def __init__(
        self,
        *,
        label: str = "stub",
        tools: Sequence[str] = ("alpha", "beta"),
        results: Mapping[str, object] | None = None,
        list_delay_ms: int = 0,
    ) -> None:
        self.label = label
        self.tools = tuple(tools)
        self.results = dict(results or {})
        self.list_delay_ms = list_delay_ms
        self.calls: list[tuple[str, Mapping[str, Any]]] = []
        self.listed = 0

    async def list_tools(self) -> tuple[str, ...]:
        """Return the tool names the stub server exposes."""
        self.listed += 1
        if self.list_delay_ms:
            await asyncio.sleep(self.list_delay_ms / 1000)
        return self.tools

    async def call_tool(self, name: str, arguments: Mapping[str, Any]) -> object:
        """Record the invocation and return the scripted result."""
        self.calls.append((name, dict(arguments)))
        return self.results.get(name)


class InterleavingSensitiveSession:
    """Session that returns the wrong answer if two calls ever interleave.

    Each call writes its token into a single shared slot, awaits a real
    suspension point and reads the slot back.  Serialised access returns each
    caller its own token; interleaved access returns the last writer's token
    to everybody, which is exactly the poisoned-session failure T078 guards.

    Args:
        delay_ms: Suspension inside the critical section, in milliseconds.
    """

    def __init__(self, *, delay_ms: int = 10) -> None:
        self._delay_s = delay_ms / 1000
        self._slot: str = ""
        self._busy = False
        self.interleaved = False
        self.started: list[str] = []
        self.completed: list[str] = []

    async def list_tools(self) -> tuple[str, ...]:
        """Return a fixed tool name; the poisoning test never filters."""
        return ("echo",)

    async def call_tool(self, name: str, arguments: Mapping[str, Any]) -> object:
        """Echo ``arguments['token']`` back, unless two calls overlapped."""
        del name
        token = str(arguments["token"])
        if self._busy:
            self.interleaved = True
        self._busy = True
        self.started.append(token)
        self._slot = token
        try:
            await asyncio.sleep(self._delay_s)
            self.completed.append(token)
            return self._slot
        finally:
            self._busy = False


class StubMcpClient:
    """Async context manager standing in for one live MCP client.

    Args:
        label: Name recorded in ``log`` on open and on close.
        session: Session handed to the runtime once connected.
        log: Shared lifecycle log receiving ``open:<label>``/``close:<label>``.
        connect_delay_ms: Delay before the connection is considered open.
        never_connects: When true the connection never completes, standing in
            for an unreachable server.
    """

    def __init__(
        self,
        *,
        label: str,
        session: object,
        log: list[str],
        connect_delay_ms: int = 0,
        never_connects: bool = False,
    ) -> None:
        self.label = label
        self.session = session
        self.log = log
        self.connect_delay_ms = connect_delay_ms
        self.never_connects = never_connects

    async def __aenter__(self) -> object:
        """Connect the stub and record the open in the shared log."""
        if self.never_connects:
            await asyncio.Event().wait()
        if self.connect_delay_ms:
            await asyncio.sleep(self.connect_delay_ms / 1000)
        self.log.append(f"open:{self.label}")
        return self.session

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Record the close in the shared log."""
        self.log.append(f"close:{self.label}")


def mcp_client_factory(
    clients: Mapping[str, StubMcpClient],
) -> Callable[[CompiledMcpCapability], StubMcpClient]:
    """Build an ``McpClientFactory`` resolving a stub client per URL.

    Args:
        clients: Stub client per ``CompiledMcpCapability.url``.

    Returns:
        The factory the runtime calls once per MCP capability.
    """

    def _factory(capability: CompiledMcpCapability) -> StubMcpClient:
        return clients[capability.url]

    return _factory


# ---------------------------------------------------------------------------
# Engine stubs
# ---------------------------------------------------------------------------


def default_script(output: object = DEFAULT_OUTPUT) -> tuple[AgentEvent, ...]:
    """Return the canonical success script: one delta plus one ``final``."""
    return (TextDeltaEvent(text="ok"), FinalEvent(output=output, usage=DEFAULT_USAGE))


class ScriptedEngine:
    """Offline :class:`~loom.ai.abc.AgentEngine` double with explicit delays.

    ``FakeAgentEngine`` replays a script instantly, which cannot express the
    timing the limit and cancellation tests need.  This double adds a declared
    per-event delay and records whether the stream observed cancellation.

    Args:
        script: Events to replay, terminal event last.
        delays_ms: Delay before each event, positionally aligned with
            ``script``; missing positions default to ``0``.
        health_status: Status reported by :meth:`health`.
        health_gate: When set, :meth:`health` waits on it before answering, so
            a test can hold the background probe open indefinitely.
    """

    def __init__(
        self,
        *,
        script: Sequence[AgentEvent] | None = None,
        delays_ms: Sequence[int] = (),
        health_status: str = "ok",
        health_gate: asyncio.Event | None = None,
    ) -> None:
        self.script: tuple[AgentEvent, ...] = tuple(
            script if script is not None else default_script()
        )
        self.delays_ms = tuple(delays_ms)
        self.health_status = health_status
        self.health_gate = health_gate
        self.emitted: list[AgentEvent] = []
        self.cancelled = False
        self.started = asyncio.Event()
        self.stream_count = 0

    def _delay_for(self, position: int) -> float:
        if position < len(self.delays_ms):
            return self.delays_ms[position] / 1000
        return 0.0

    async def _iterate(self) -> AsyncGenerator[AgentEvent, None]:
        try:
            for position, event in enumerate(self.script):
                delay = self._delay_for(position)
                if delay:
                    await asyncio.sleep(delay)
                self.started.set()
                self.emitted.append(event)
                yield event
        except asyncio.CancelledError:
            self.cancelled = True
            raise

    def run_stream(
        self, prompt: str, *, identity: Identity
    ) -> Any:  # AbstractAsyncContextManager[AsyncIterator[AgentEvent]]
        """Replay the script as an event stream."""
        del prompt, identity
        self.stream_count += 1

        @asynccontextmanager
        async def _stream() -> AsyncIterator[AsyncIterator[AgentEvent]]:
            iterator = self._iterate()
            try:
                yield iterator
            finally:
                await iterator.aclose()

        return _stream()

    async def run(self, prompt: str, *, identity: Identity) -> AgentResult:
        """Replay the script to completion and return its terminal outcome."""
        async with self.run_stream(prompt, identity=identity) as stream:
            last: AgentEvent | None = None
            async for event in stream:
                last = event
        if isinstance(last, FinalEvent):
            return AgentResult(output=last.output, usage=last.usage)
        raise AssertionError("scripted run did not end in a FinalEvent")

    async def health(self) -> HealthStatus:
        """Report the scripted health, optionally blocking on the gate."""
        if self.health_gate is not None:
            await self.health_gate.wait()
        return HealthStatus(status=self.health_status)  # type: ignore[arg-type]


class CountingEngineProvider:
    """Engine provider counting how often the runtime builds an engine.

    Args:
        engines: Engine per plan name.  A plan absent from the mapping gets a
            fresh default-script :class:`ScriptedEngine`.
    """

    LOOM_AI_ENGINE_API = 1

    def __init__(self, engines: Mapping[str, ScriptedEngine] | None = None) -> None:
        self.engines: dict[str, ScriptedEngine] = dict(engines or {})
        self.calls: list[str] = []

    def create_engine(self, plan: object, *, deps: object, container: object) -> ScriptedEngine:
        """Build (or return the pre-scripted) engine for one plan."""
        del deps, container
        name = str(getattr(plan, "name", plan))
        self.calls.append(name)
        return self.engines.setdefault(name, ScriptedEngine())

    def supported_capability_kinds(self) -> frozenset[str]:
        """Accept every compiled capability kind."""
        return frozenset({"usecase", "sql", "mcp", "skills", "python", "a2a"})


class StubDepsFactory:
    """Per-invocation dependency factory carrying only the caller identity."""

    def build(self, identity: Identity, container: LoomContainer) -> object:
        """Return the dependency bundle for one invocation."""
        del container
        return {"identity": identity}


# ---------------------------------------------------------------------------
# Plan and configuration builders
# ---------------------------------------------------------------------------


def make_policies(
    *,
    retries: int = 0,
    tool_timeout_ms: int = 1000,
    max_iterations: int = 8,
    run_timeout_ms: int = 5000,
) -> PolicySpec:
    """Build a :class:`PolicySpec` with every limit stated explicitly."""
    return PolicySpec(
        retries=retries,
        tool_timeout_ms=tool_timeout_ms,
        max_iterations=max_iterations,
        run_timeout_ms=run_timeout_ms,
    )


def make_plan(
    name: str = "analyst",
    *,
    capabilities: Sequence[CompiledCapability] = (),
    policies: PolicySpec | None = None,
) -> AgentPlan:
    """Build a compiled plan with a decodable output and no secret material."""
    return AgentPlan(
        name=name,
        description=f"{name} test agent",
        instructions="answer",
        spec_version=1,
        inference=InferenceTarget(provider="fake", model="fake-model"),
        output=CompiledOutput(
            schema={"type": "object"},
            decoder=msgspec.json.Decoder(dict),
        ),
        capabilities=tuple(capabilities),
        policies=policies if policies is not None else make_policies(),
        metadata={},
    )


def make_mcp_capability(
    url: str = "https://tools.internal/mcp",
    *,
    include: Sequence[str] = (),
    exclude: Sequence[str] = (),
) -> CompiledMcpCapability:
    """Build an MCP capability, optionally carrying a tool filter."""
    from loom.ai.declarative import ToolFilter

    tool_filter = (
        ToolFilter(include=tuple(include), exclude=tuple(exclude)) if include or exclude else None
    )
    return CompiledMcpCapability(url=url, tool_filter=tool_filter)


def make_sql_connection(*, readonly: bool = True) -> SqlConnectionConfig:
    """Build a SQL connection configuration with an explicit read-only state."""
    return SqlConnectionConfig(
        backend="clickhouse",
        url="clickhouse://reports.internal:8123/reporting",
        allowed_roles=(),
        readonly=readonly,
    )


def make_sql_capability(
    connection: str = "reporting",
    *,
    readonly: bool = True,
) -> CompiledSqlCapability:
    """Build a compiled SQL capability pinning the plan's read-only belief."""
    return CompiledSqlCapability(
        connection=connection,
        config=make_sql_connection(readonly=readonly),
        max_rows=1000,
        max_result_bytes=1_000_000,
    )


def make_sql_config(connection: str = "reporting", *, readonly: bool = True) -> SqlConfig:
    """Build the live SQL configuration the runtime re-verifies against."""
    return SqlConfig(connections={connection: make_sql_connection(readonly=readonly)})


def make_ai_config(
    *,
    endpoints: Mapping[str, AgentEndpointConfig] | None = None,
    a2a: A2AConfig | None = None,
    startup_timeout_ms: int = 500,
    max_concurrent_runs: int = 8,
    max_prompt_bytes: int = 65536,
    health_cache_ttl_ms: int = 20,
) -> AiConfig:
    """Build an ``AiConfig`` with test-sized budgets and no model secrets."""
    return AiConfig(
        engine="fake",
        specs=("agents/*.agent.yaml",),
        models={"default": InferenceTarget(provider="fake", model="fake-model")},
        endpoints=dict(endpoints or {}),
        a2a=a2a,
        startup_timeout_ms=startup_timeout_ms,
        max_concurrent_runs=max_concurrent_runs,
        max_prompt_bytes=max_prompt_bytes,
        health_cache_ttl_ms=health_cache_ttl_ms,
    )


def make_endpoint(
    *,
    enabled: bool = True,
    auth: str = "external",
    allow_anonymous: bool = False,
) -> AgentEndpointConfig:
    """Build one per-agent HTTP opt-in entry."""
    return AgentEndpointConfig(enabled=enabled, auth=auth, allow_anonymous=allow_anonymous)


def error_script(
    code: AgentRunErrorCode, message: str = "scripted failure"
) -> tuple[AgentEvent, ...]:
    """Return a script whose terminal event is an ``ErrorEvent`` with ``code``."""
    return (ErrorEvent(code=code, message=message),)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def identity() -> Identity:
    """Verified caller used by every run in this suite."""
    return Identity(subject="user-1", roles=("analyst",), mechanism="test")


@pytest.fixture
def container() -> LoomContainer:
    """Empty application container; no test resolves anything from it."""
    return LoomContainer()


@pytest.fixture
def deps() -> StubDepsFactory:
    """Per-invocation dependency factory shared by every runtime under test."""
    return StubDepsFactory()


@pytest.fixture
def lifecycle_log() -> list[str]:
    """Shared open/close log every stub MCP client appends to."""
    return []
