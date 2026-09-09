"""``ai.max_agent_depth``: cycle and depth bounds over the in-flight run chain (T203/T204).

Every run started through ``AgentRuntime.run`` pushes its own name onto a
task-local chain and pops it in a ``finally`` — this is what a marker-driven
nested run reuses unmodified, so the coverage lives here, one level below the
``AgentHandle`` adapter, where the mechanism this test protects actually
lives.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

import pytest

from loom.ai.abc import AgentEvent, AgentResult, Conversation, FinalEvent, HealthStatus
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.runtime import AgentRuntime
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from tests.integration.ai.conftest import (
    DEFAULT_USAGE,
    CountingEngineProvider,
    StubDepsFactory,
    make_ai_config,
    make_plan,
)

_OUTER = "outer"
_INNER = "inner"


class _CallbackEngine:
    """Runs one scripted turn, invoking *callback* (if any) before finishing.

    The callback is where a test drives a nested ``runtime.run`` call from
    inside an in-flight one — the same position a marker-resolved
    ``AgentHandle.run`` call would run from inside a use case a hook or a
    tool invokes.
    """

    def __init__(self, callback: Callable[[], Awaitable[None]] | None = None) -> None:
        self._callback = callback

    def run_stream(
        self,
        prompt: str,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
    ) -> AbstractAsyncContextManager[AsyncIterator[AgentEvent]]:
        del prompt, identity, conversation

        async def _events() -> AsyncIterator[AgentEvent]:
            if self._callback is not None:
                await self._callback()
            yield FinalEvent(output={}, usage=DEFAULT_USAGE)

        @asynccontextmanager
        async def _stream() -> AsyncIterator[AsyncIterator[AgentEvent]]:
            yield _events()

        return _stream()

    async def run(
        self,
        prompt: str,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
    ) -> AgentResult:
        last: AgentEvent | None = None
        async with self.run_stream(prompt, identity=identity, conversation=conversation) as events:
            async for event in events:
                last = event
        assert isinstance(last, FinalEvent)
        return AgentResult(output=last.output, usage=last.usage)

    async def health(self) -> HealthStatus:
        return HealthStatus(status="ok")


def _runtime(
    *,
    max_agent_depth: int,
    outer_callback: Callable[[], Awaitable[None]] | None,
    deps: StubDepsFactory,
    container: LoomContainer,
) -> AgentRuntime:
    outer_engine = _CallbackEngine(outer_callback)
    inner_engine = _CallbackEngine()
    engines = {_OUTER: outer_engine, _INNER: inner_engine}
    provider = CountingEngineProvider(engines=engines)  # type: ignore[arg-type]
    return AgentRuntime(
        plans=[make_plan(_OUTER), make_plan(_INNER)],
        config=make_ai_config(max_agent_depth=max_agent_depth),
        engine_provider=provider,  # type: ignore[arg-type]
        deps=deps,
        container=container,
    )


class TestProfundidadPorDefecto:
    """Con ``max_agent_depth`` en su valor por defecto de uno."""

    async def test_una_corrida_anidada_de_otro_agente_excede_la_profundidad(
        self,
        identity: Identity,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """La corrida raíz ya agota el presupuesto por defecto de uno."""

        async def _nest() -> None:
            with pytest.raises(AgentRunError) as excinfo:
                await runtime.run(_INNER, "hola", identity=identity)
            assert excinfo.value.code is AgentRunErrorCode.AGENT_CALL_TOO_DEEP
            # Pins the exact message docs/rest/use-case-dsl.md quotes for
            # AGENT_CALL_TOO_DEEP; edit one without the other and this is the
            # gap the next review catches.
            assert str(excinfo.value) == (
                "agent call chain outer -> inner exceeds ai.max_agent_depth=1"
            )

        runtime = _runtime(max_agent_depth=1, outer_callback=_nest, deps=deps, container=container)
        async with runtime:
            await runtime.run(_OUTER, "hola", identity=identity)

    async def test_una_corrida_anidada_del_mismo_agente_es_un_ciclo(
        self,
        identity: Identity,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """El mismo nombre en la cadena se reporta como ciclo, no como profundidad."""

        async def _nest() -> None:
            with pytest.raises(AgentRunError) as excinfo:
                await runtime.run(_OUTER, "hola", identity=identity)
            assert excinfo.value.code is AgentRunErrorCode.AGENT_CALL_CYCLE
            assert _OUTER in str(excinfo.value)
            # Pins the exact message docs/rest/use-case-dsl.md quotes for
            # AGENT_CALL_CYCLE.
            assert str(excinfo.value) == "agent call cycle detected: outer -> outer"

        runtime = _runtime(max_agent_depth=5, outer_callback=_nest, deps=deps, container=container)
        async with runtime:
            await runtime.run(_OUTER, "hola", identity=identity)

    async def test_dos_corridas_seguidas_del_mismo_agente_no_se_contaminan(
        self,
        identity: Identity,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """La cadena se restaura en el 'finally': dos corridas no anidadas no chocan.

        Si la restauración se perdiera, la segunda corrida vería el nombre de
        la primera todavía en la cadena y fallaría como un ciclo espurio.
        """
        runtime = _runtime(max_agent_depth=1, outer_callback=None, deps=deps, container=container)
        async with runtime:
            await runtime.run(_OUTER, "hola", identity=identity)
            await runtime.run(_OUTER, "hola de nuevo", identity=identity)

    async def test_una_corrida_fallida_tambien_restaura_la_cadena(
        self,
        identity: Identity,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Un fallo dentro de la corrida no deja el nombre atascado en la cadena."""

        calls = 0

        async def _fail_once() -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("boom")

        runtime = _runtime(
            max_agent_depth=1, outer_callback=_fail_once, deps=deps, container=container
        )
        async with runtime:
            with pytest.raises(RuntimeError, match="boom"):
                await runtime.run(_OUTER, "hola", identity=identity)
            # Same agent, same task: succeeds only if the failed run's name
            # was popped from the chain despite the exception.
            await runtime.run(_OUTER, "otra vez", identity=identity)


class TestProfundidadElevada:
    """Con ``max_agent_depth`` elevado explícitamente por el operador."""

    async def test_una_corrida_anidada_de_otro_agente_cabe_en_el_presupuesto(
        self,
        identity: Identity,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        seen: list[str] = []

        async def _nest() -> None:
            result = await runtime.run(_INNER, "hola", identity=identity)
            seen.append("nested-ok")
            assert result.output == {}

        runtime = _runtime(max_agent_depth=2, outer_callback=_nest, deps=deps, container=container)
        async with runtime:
            await runtime.run(_OUTER, "hola", identity=identity)

        assert seen == ["nested-ok"]
