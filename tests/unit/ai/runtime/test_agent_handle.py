"""``_BoundAgentHandle``: the AgentHandle a marker resolves to (T203/T205).

Covers the anonymous-caller refusal, the handle's own span (nothing else
opens one on this code path — see the module docstring of ``_handle.py``),
and the not-yet-implemented run modes reserved for a later pull request.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from loom.ai.abc import AgentEvent, AgentResult, Conversation, FinalEvent, HealthStatus
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.runtime import AgentRuntime
from loom.ai.runtime._handle import _BoundAgentHandle, agent_marker_resolver
from loom.core.di import LoomContainer
from loom.core.identity import ANONYMOUS, Identity
from loom.core.observability.runtime import ObservabilityRuntime
from tests.integration.ai.conftest import (
    DEFAULT_USAGE,
    CountingEngineProvider,
    StubDepsFactory,
    make_ai_config,
    make_plan,
)

_AUTHENTICATED = Identity(subject="ada", mechanism="test")
_AGENT_NAME = "triage"


@pytest.fixture
def deps() -> StubDepsFactory:
    """Per-invocation dependency factory carrying only the caller identity."""
    return StubDepsFactory()


@pytest.fixture
def container() -> LoomContainer:
    """Empty application container; no test here resolves anything from it."""
    return LoomContainer()


class _OneShotEngine:
    """A single successful turn, for tests that only need the happy path."""

    def __init__(self) -> None:
        self.run_stream_calls = 0
        self.identities: list[Identity] = []

    def run_stream(
        self,
        prompt: str,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
    ) -> object:
        del prompt, conversation
        self.run_stream_calls += 1
        self.identities.append(identity)

        @asynccontextmanager
        async def _stream() -> AsyncIterator[AsyncIterator[AgentEvent]]:
            async def _events() -> AsyncIterator[AgentEvent]:
                yield FinalEvent(output={"ok": True}, usage=DEFAULT_USAGE)

            yield _events()

        return _stream()

    async def run(
        self, prompt: str, *, identity: Identity, conversation: Conversation | None = None
    ) -> AgentResult:
        del prompt, identity, conversation
        return AgentResult(output={"ok": True}, usage=DEFAULT_USAGE)

    async def health(self) -> HealthStatus:
        return HealthStatus(status="ok")


def _tracing_runtime() -> tuple[ObservabilityRuntime, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return ObservabilityRuntime([], tracer=provider.get_tracer("loom.ai")), exporter


async def _agent_runtime(deps: StubDepsFactory, container: LoomContainer) -> AgentRuntime:
    provider = CountingEngineProvider(engines={_AGENT_NAME: _OneShotEngine()})  # type: ignore[dict-item]
    return AgentRuntime(
        plans=[make_plan(_AGENT_NAME)],
        config=make_ai_config(),
        engine_provider=provider,  # type: ignore[arg-type]
        deps=deps,
        container=container,
    )


class TestIdentidadAnonima:
    """El llamante anónimo se rechaza antes de tocar el modelo (design R3)."""

    async def test_un_llamante_anonimo_se_rechaza_con_unauthorized(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        runtime = await _agent_runtime(deps, container)
        handle = _BoundAgentHandle(
            name=_AGENT_NAME, runtime=runtime, identity=ANONYMOUS, observability=None
        )

        async with runtime:
            with pytest.raises(AgentRunError) as excinfo:
                await handle.run("hola")

        assert excinfo.value.code is AgentRunErrorCode.UNAUTHORIZED

    async def test_un_llamante_anonimo_nunca_llega_al_modelo(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """El motor nunca se invoca: la corrida se corta antes del 'run' del runtime."""
        engine = _OneShotEngine()
        provider = CountingEngineProvider(engines={_AGENT_NAME: engine})  # type: ignore[dict-item]
        runtime = AgentRuntime(
            plans=[make_plan(_AGENT_NAME)],
            config=make_ai_config(),
            engine_provider=provider,  # type: ignore[arg-type]
            deps=deps,
            container=container,
        )
        handle = _BoundAgentHandle(
            name=_AGENT_NAME, runtime=runtime, identity=ANONYMOUS, observability=None
        )

        async with runtime:
            with pytest.raises(AgentRunError):
                await handle.run("hola")
            assert engine.run_stream_calls == 0

    async def test_un_llamante_autenticado_corre_como_si_mismo(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """La corrida del motor recibe exactamente la identidad del handle,

        nunca otra: el runtime autoriza cada capacidad con esta identidad, así
        que forzarla a otra por dentro del adaptador dejaría a la capacidad
        corriendo como un tercero.
        """
        engine = _OneShotEngine()
        provider = CountingEngineProvider(engines={_AGENT_NAME: engine})  # type: ignore[dict-item]
        runtime = AgentRuntime(
            plans=[make_plan(_AGENT_NAME)],
            config=make_ai_config(),
            engine_provider=provider,  # type: ignore[arg-type]
            deps=deps,
            container=container,
        )
        handle = _BoundAgentHandle(
            name=_AGENT_NAME, runtime=runtime, identity=_AUTHENTICATED, observability=None
        )

        async with runtime:
            answer = await handle.run("hola")

        assert answer.output == {"ok": True}
        assert engine.identities == [_AUTHENTICATED]


class TestElSpanDelHandle:
    """El handle abre su propio span; el runtime no abre ninguno por su cuenta."""

    async def test_una_corrida_exitosa_abre_y_cierra_un_span_de_agente(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        observability, exporter = _tracing_runtime()
        runtime = AgentRuntime(
            plans=[make_plan(_AGENT_NAME)],
            config=make_ai_config(),
            engine_provider=CountingEngineProvider(  # type: ignore[arg-type]
                engines={_AGENT_NAME: _OneShotEngine()}  # type: ignore[dict-item]
            ),
            deps=deps,
            container=container,
        )
        handle = _BoundAgentHandle(
            name=_AGENT_NAME, runtime=runtime, identity=_AUTHENTICATED, observability=observability
        )

        async with runtime:
            answer = await handle.run("hola")

        spans = exporter.get_finished_spans()
        assert [span.name for span in spans] == ["agent:agent_run"]
        assert spans[0].attributes is not None
        assert spans[0].attributes["subject"] == "ada"
        assert spans[0].attributes["interaction_id"] == answer.interaction_id

    async def test_sin_runtime_de_observabilidad_no_hay_ningun_span(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """``observability=None`` es un no-op explícito, no un fallo silencioso."""
        runtime = await _agent_runtime(deps, container)
        handle = _BoundAgentHandle(
            name=_AGENT_NAME, runtime=runtime, identity=_AUTHENTICATED, observability=None
        )

        async with runtime:
            answer = await handle.run("hola")

        assert answer.output == {"ok": True}


class TestModosTodaviaNoImplementados:
    """Lo que esta PR deja explícitamente para la PR3."""

    async def test_expect_no_implementado(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        runtime = await _agent_runtime(deps, container)
        handle = _BoundAgentHandle(
            name=_AGENT_NAME, runtime=runtime, identity=_AUTHENTICATED, observability=None
        )
        async with runtime:
            with pytest.raises(NotImplementedError):
                await handle.run("hola", expect=dict)

    async def test_run_text_no_implementado(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        runtime = await _agent_runtime(deps, container)
        handle = _BoundAgentHandle(
            name=_AGENT_NAME, runtime=runtime, identity=_AUTHENTICATED, observability=None
        )
        with pytest.raises(NotImplementedError):
            await handle.run_text("hola")

    def test_mcp_no_implementado(self, deps: StubDepsFactory, container: LoomContainer) -> None:
        handle = _BoundAgentHandle(
            name=_AGENT_NAME,
            runtime=object(),  # type: ignore[arg-type]
            identity=_AUTHENTICATED,
            observability=None,
        )
        with pytest.raises(NotImplementedError):
            handle.mcp("server")

    def test_sql_no_implementado(self, deps: StubDepsFactory, container: LoomContainer) -> None:
        handle = _BoundAgentHandle(
            name=_AGENT_NAME,
            runtime=object(),  # type: ignore[arg-type]
            identity=_AUTHENTICATED,
            observability=None,
        )
        with pytest.raises(NotImplementedError):
            handle.sql("connection")

    def test_grants_no_implementado(self, deps: StubDepsFactory, container: LoomContainer) -> None:
        handle = _BoundAgentHandle(
            name=_AGENT_NAME,
            runtime=object(),  # type: ignore[arg-type]
            identity=_AUTHENTICATED,
            observability=None,
        )
        with pytest.raises(NotImplementedError):
            handle.grants()


class TestElResolverDeMarcadores:
    """``agent_marker_resolver`` construye el callable que el executor toma."""

    async def test_el_resolver_liga_el_nombre_y_el_llamante(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        runtime = await _agent_runtime(deps, container)
        resolve = agent_marker_resolver(runtime, observability=None)

        handle = resolve(_AGENT_NAME, _AUTHENTICATED)

        async with runtime:
            answer = await handle.run("hola")

        assert answer.output == {"ok": True}
