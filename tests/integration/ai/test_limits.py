"""Per-run and per-worker bounds enforced by ``AgentRuntime`` (T082, FR-033a).

Four bounds, one code each: ``max_concurrent_runs`` → ``TOO_MANY_RUNS``,
``run_timeout_ms`` → ``RUN_TIMEOUT``, ``max_iterations`` →
``MAX_ITERATIONS_EXCEEDED`` and ``tool_timeout_ms`` → ``TOOL_TIMEOUT``.  Every
bound is driven by a scripted engine with declared delays: no clock is patched
and no timeout is waited out for longer than the budget under test.
"""

from __future__ import annotations

import asyncio

import pytest

from loom.ai.abc import AgentEvent, FinalEvent, TextDeltaEvent, ToolCallEvent, ToolResultEvent
from loom.ai.errors import AgentRunErrorCode
from loom.ai.runtime import AgentRunError, AgentRuntime
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from tests.integration.ai.conftest import (
    DEFAULT_USAGE,
    CountingEngineProvider,
    ScriptedEngine,
    StubDepsFactory,
    make_ai_config,
    make_plan,
    make_policies,
)

_AGENT = "analyst"


def _runtime(
    engine: ScriptedEngine,
    *,
    deps: StubDepsFactory,
    container: LoomContainer,
    max_concurrent_runs: int = 8,
    tool_timeout_ms: int = 1000,
    max_iterations: int = 8,
    run_timeout_ms: int = 5000,
) -> AgentRuntime:
    """Build a runtime with one scripted agent and explicit limits."""
    plan = make_plan(
        _AGENT,
        policies=make_policies(
            tool_timeout_ms=tool_timeout_ms,
            max_iterations=max_iterations,
            run_timeout_ms=run_timeout_ms,
        ),
    )
    return AgentRuntime(
        plans=[plan],
        config=make_ai_config(max_concurrent_runs=max_concurrent_runs),
        engine_provider=CountingEngineProvider(engines={_AGENT: engine}),  # type: ignore[arg-type]
        deps=deps,
        container=container,
    )


def _tool_script(iterations: int) -> tuple[AgentEvent, ...]:
    """Script performing ``iterations`` complete tool cycles before ``final``."""
    events: list[AgentEvent] = []
    for index in range(iterations):
        call_id = f"c{index}"
        events.append(ToolCallEvent(tool="sales:velocity", call_id=call_id, arguments={"n": index}))
        events.append(ToolResultEvent(call_id=call_id, ok=True, summary="ok"))
    events.append(FinalEvent(output={"answer": "done"}, usage=DEFAULT_USAGE))
    return tuple(events)


class TestRunsConcurrentes:
    """``max_concurrent_runs`` is a per-worker admission bound, not a queue."""

    async def test_rechaza_con_too_many_runs_cuando_el_cupo_esta_agotado(
        self,
        identity: Identity,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A second run with the single slot taken is refused, not admitted."""
        engine = ScriptedEngine(delays_ms=(0, 40))
        runtime = _runtime(engine, deps=deps, container=container, max_concurrent_runs=1)

        async with runtime:
            first = asyncio.create_task(runtime.run(_AGENT, "p1", identity=identity))
            await engine.started.wait()
            with pytest.raises(AgentRunError) as failure:
                await runtime.run(_AGENT, "p2", identity=identity)
            await first

        assert failure.value.code is AgentRunErrorCode.TOO_MANY_RUNS

    async def test_falla_de_inmediato_cuando_el_cupo_esta_agotado(
        self,
        identity: Identity,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Refusal is immediate: the caller never waits for the run in flight."""
        engine = ScriptedEngine(delays_ms=(0, 40))
        runtime = _runtime(engine, deps=deps, container=container, max_concurrent_runs=1)
        loop = asyncio.get_running_loop()

        async with runtime:
            first = asyncio.create_task(runtime.run(_AGENT, "p1", identity=identity))
            await engine.started.wait()
            started = loop.time()
            with pytest.raises(AgentRunError):
                await runtime.run(_AGENT, "p2", identity=identity)
            elapsed = loop.time() - started
            await first

        assert elapsed < 0.020, f"the refusal queued for {elapsed:.3f}s instead of failing fast"


class TestPresupuestoDeEjecucion:
    """``run_timeout_ms`` is a wall-clock budget for the whole run."""

    async def test_termina_con_run_timeout_cuando_el_stream_es_lento(
        self,
        identity: Identity,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A stream slower than the budget ends with ``RUN_TIMEOUT``."""
        engine = ScriptedEngine(delays_ms=(0, 400))
        runtime = _runtime(engine, deps=deps, container=container, run_timeout_ms=30)

        async with runtime:
            with pytest.raises(AgentRunError) as failure:
                await runtime.run(_AGENT, "prompt", identity=identity)

        assert failure.value.code is AgentRunErrorCode.RUN_TIMEOUT


class TestIteraciones:
    """``max_iterations`` counts tool calls and names the limit it enforces."""

    async def test_termina_con_max_iterations_cuando_hay_demasiadas_llamadas(
        self,
        identity: Identity,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Three tool cycles under a limit of one end with the limit code."""
        engine = ScriptedEngine(script=_tool_script(3))
        runtime = _runtime(engine, deps=deps, container=container, max_iterations=1)

        async with runtime:
            with pytest.raises(AgentRunError) as failure:
                await runtime.run(_AGENT, "prompt", identity=identity)

        assert failure.value.code is AgentRunErrorCode.MAX_ITERATIONS_EXCEEDED

    async def test_nombra_el_limite_cuando_agota_las_iteraciones(
        self,
        identity: Identity,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The failure message states the limit that stopped the run."""
        engine = ScriptedEngine(script=_tool_script(3))
        runtime = _runtime(engine, deps=deps, container=container, max_iterations=2)

        async with runtime:
            with pytest.raises(AgentRunError) as failure:
                await runtime.run(_AGENT, "prompt", identity=identity)

        assert "2" in str(failure.value)


class TestTimeoutDeHerramienta:
    """``tool_timeout_ms`` bounds the gap between a call and its result."""

    async def test_termina_con_tool_timeout_cuando_la_herramienta_se_estanca(
        self,
        identity: Identity,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A stall between ``tool_call`` and ``tool_result`` ends with ``TOOL_TIMEOUT``."""
        script: tuple[AgentEvent, ...] = (
            TextDeltaEvent(text="thinking"),
            ToolCallEvent(tool="sales:velocity", call_id="c1", arguments={}),
            ToolResultEvent(call_id="c1", ok=True, summary="142 rows"),
            FinalEvent(output={"answer": "done"}, usage=DEFAULT_USAGE),
        )
        engine = ScriptedEngine(script=script, delays_ms=(0, 0, 400, 0))
        runtime = _runtime(engine, deps=deps, container=container, tool_timeout_ms=20)

        async with runtime:
            with pytest.raises(AgentRunError) as failure:
                await runtime.run(_AGENT, "prompt", identity=identity)

        assert failure.value.code is AgentRunErrorCode.TOOL_TIMEOUT

    async def test_nombra_la_herramienta_cuando_expira_el_timeout(
        self,
        identity: Identity,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The failure message names the tool that never answered."""
        script: tuple[AgentEvent, ...] = (
            ToolCallEvent(tool="sales:velocity", call_id="c1", arguments={}),
            ToolResultEvent(call_id="c1", ok=True, summary="142 rows"),
            FinalEvent(output={"answer": "done"}, usage=DEFAULT_USAGE),
        )
        engine = ScriptedEngine(script=script, delays_ms=(0, 400, 0))
        runtime = _runtime(engine, deps=deps, container=container, tool_timeout_ms=20)

        async with runtime:
            with pytest.raises(AgentRunError) as failure:
                await runtime.run(_AGENT, "prompt", identity=identity)

        assert "sales:velocity" in str(failure.value)
