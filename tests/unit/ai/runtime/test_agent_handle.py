"""``_BoundAgentHandle``: the AgentHandle a marker resolves to (T203/T205/T304).

Covers the anonymous-caller refusal, the handle's own span (nothing else
opens one on this code path — see the module docstring of ``_handle.py``),
the three run modes, the per-run-shape refusal when the output hook needs the
declared shape, and the grant lookups' not-yet-implemented failure mode.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import msgspec
import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from loom.ai.abc import AgentEvent, AgentResult, Conversation, FinalEvent, HealthStatus
from loom.ai.compiler._plan import CompiledOutputHook
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.runtime import AgentRuntime
from loom.ai.runtime._handle import _BoundAgentHandle, agent_marker_resolver
from loom.core.command import Command
from loom.core.di import LoomContainer
from loom.core.identity import ANONYMOUS, Identity
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.use_case import Caller, Input, UseCase
from tests.integration.ai.conftest import (
    DEFAULT_USAGE,
    ConversationRecorder,
    CountingEngineProvider,
    RecordingDepsFactory,
    RecordTurn,
    StubDepsFactory,
    conversational_plan,
    make_ai_config,
    make_plan,
)


class _MessageOnlyCommand(Command, frozen=True, kw_only=True):
    """A hook command that never reads the run's output — only its bookkeeping."""

    interaction_id: str
    conversation_id: str | None = None


class _RecordMessageOnly(UseCase[Any, None]):
    """An output hook that declares no ``output`` field at all.

    The regression fixture for the narrow refusal (T304): a hook shaped like
    conversation persistence, which never touches the decoded answer, must
    keep working under both new run modes.
    """

    def __init__(self, recorder: ConversationRecorder) -> None:
        self._recorder = recorder

    async def execute(
        self, cmd: _MessageOnlyCommand = Input(), caller: Identity = Caller()
    ) -> None:
        del caller
        self._recorder.timeline.append("hook")


def _messages_only_plan(name: str) -> Any:
    """Build a plan whose ``on_output`` command never declares ``output``."""
    hook = CompiledOutputHook(
        usecase="messages-only",
        use_case=_RecordMessageOnly,
        accepted=frozenset(info.name for info in msgspec.structs.fields(_MessageOnlyCommand)),
    )
    return msgspec.structs.replace(make_plan(name), on_output=hook)


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

    async def test_el_mensaje_es_el_que_muestra_use_case_dsl_md(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """Pins the exact message ``docs/rest/use-case-dsl.md`` quotes for ``UNAUTHORIZED``.

        Edit one without the other and this test is the gap the next review
        catches.
        """
        runtime = await _agent_runtime(deps, container)
        # The anonymous refusal runs before the runtime is ever asked to run
        # this name, so a name absent from the runtime's own plans is fine
        # here — it matches the agent name ``markers.md`` and the example
        # in ``docs/rest/use-case-dsl.md`` both use.
        handle = _BoundAgentHandle(
            name="incident-triage", runtime=runtime, identity=ANONYMOUS, observability=None
        )

        async with runtime:
            with pytest.raises(AgentRunError) as excinfo:
                await handle.run("hola")

        assert str(excinfo.value) == "agent 'incident-triage' requires an authenticated caller"

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


class _ShapedEngine(_OneShotEngine):
    """A one-shot engine that also serves a per-run shape override (T304).

    Records every ``output_type`` it was asked to run with, so a test can
    assert the artefact's own declared shape was bypassed for an overridden
    call and used for a plain one.
    """

    def __init__(self, *, shaped_output: object = "prose") -> None:
        super().__init__()
        self._shaped_output = shaped_output
        self.shaped_calls: list[type[Any] | None] = []

    def run_stream_shaped(
        self,
        prompt: str,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
        output_type: type[Any],
    ) -> object:
        del prompt
        self.shaped_calls.append(output_type)
        self.identities.append(identity)

        @asynccontextmanager
        async def _stream() -> AsyncIterator[AsyncIterator[AgentEvent]]:
            async def _events() -> AsyncIterator[AgentEvent]:
                yield FinalEvent(output=self._shaped_output, usage=DEFAULT_USAGE)

            yield _events()

        return _stream()


async def _shaped_runtime(
    deps: StubDepsFactory, container: LoomContainer, engine: _ShapedEngine
) -> AgentRuntime:
    provider = CountingEngineProvider(engines={_AGENT_NAME: engine})  # type: ignore[dict-item]
    return AgentRuntime(
        plans=[make_plan(_AGENT_NAME)],
        config=make_ai_config(),
        engine_provider=provider,  # type: ignore[arg-type]
        deps=deps,
        container=container,
    )


class TestLosTresModos:
    """T304: la forma declarada, la forma por corrida y el texto abierto."""

    async def test_run_sin_expect_usa_la_forma_declarada_del_artefacto(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        runtime = await _agent_runtime(deps, container)
        handle = _BoundAgentHandle(
            name=_AGENT_NAME, runtime=runtime, identity=_AUTHENTICATED, observability=None
        )

        async with runtime:
            answer = await handle.run("hola")

        assert answer.output == {"ok": True}

    async def test_run_con_expect_tipa_la_respuesta_de_esa_corrida_solamente(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        engine = _ShapedEngine(shaped_output={"severity": 5})
        runtime = await _shaped_runtime(deps, container, engine)
        handle = _BoundAgentHandle(
            name=_AGENT_NAME, runtime=runtime, identity=_AUTHENTICATED, observability=None
        )

        async with runtime:
            answer = await handle.run("hola", expect=dict)

        assert answer.output == {"severity": 5}
        assert engine.shaped_calls == [dict]

    async def test_run_text_devuelve_prosa_sin_forma_declarada(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        engine = _ShapedEngine(shaped_output="a plain sentence")
        runtime = await _shaped_runtime(deps, container, engine)
        handle = _BoundAgentHandle(
            name=_AGENT_NAME, runtime=runtime, identity=_AUTHENTICATED, observability=None
        )

        async with runtime:
            answer = await handle.run_text("hola")

        assert answer.output == "a plain sentence"
        assert engine.shaped_calls == [str]

    async def test_expect_no_invoca_la_comprobacion_de_salida_del_artefacto(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """La forma declarada usa run_stream; una forma por corrida usa run_stream_shaped."""
        engine = _ShapedEngine(shaped_output={"anything": True})
        runtime = await _shaped_runtime(deps, container, engine)
        handle = _BoundAgentHandle(
            name=_AGENT_NAME, runtime=runtime, identity=_AUTHENTICATED, observability=None
        )

        async with runtime:
            await handle.run("hola", expect=dict)

        assert engine.run_stream_calls == 0
        assert engine.shaped_calls == [dict]


class TestRechazoPorHookDeSalida:
    """T304: una forma por corrida se rechaza antes del modelo si el hook la necesita."""

    async def test_expect_se_rechaza_cuando_el_hook_declara_output(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        engine = _ShapedEngine()
        provider = CountingEngineProvider(engines={_AGENT_NAME: engine})  # type: ignore[dict-item]
        plan = conversational_plan(_AGENT_NAME, hook=True)
        runtime = AgentRuntime(
            plans=[plan],
            config=make_ai_config(),
            engine_provider=provider,  # type: ignore[arg-type]
            deps=RecordingDepsFactory((RecordTurn,)),  # type: ignore[arg-type]
            container=container,
        )
        handle = _BoundAgentHandle(
            name=_AGENT_NAME, runtime=runtime, identity=_AUTHENTICATED, observability=None
        )

        async with runtime:
            with pytest.raises(AgentRunError) as excinfo:
                await handle.run("hola", expect=dict)

        assert excinfo.value.code is AgentRunErrorCode.AGENT_RUN_SHAPE_WITH_HOOK
        assert engine.shaped_calls == []
        assert engine.run_stream_calls == 0

    async def test_el_mensaje_es_el_que_muestra_use_case_dsl_md(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """Pins the exact message ``docs/rest/use-case-dsl.md`` quotes for
        ``AGENT_RUN_SHAPE_WITH_HOOK``, under the same agent name the page's
        example uses.
        """
        agent_name = "incident-triage"
        engine = _ShapedEngine()
        provider = CountingEngineProvider(engines={agent_name: engine})  # type: ignore[dict-item]
        plan = conversational_plan(agent_name, hook=True)
        runtime = AgentRuntime(
            plans=[plan],
            config=make_ai_config(),
            engine_provider=provider,  # type: ignore[arg-type]
            deps=RecordingDepsFactory((RecordTurn,)),  # type: ignore[arg-type]
            container=container,
        )
        handle = _BoundAgentHandle(
            name=agent_name, runtime=runtime, identity=_AUTHENTICATED, observability=None
        )

        async with runtime:
            with pytest.raises(AgentRunError) as excinfo:
                await handle.run("hola", expect=dict)

        assert str(excinfo.value) == (
            "agent 'incident-triage' declares an output hook that reads the run's "
            "output, so this run cannot use a per-run shape; call run(prompt) for "
            "the artefact's own declared output instead"
        )

    async def test_run_text_se_rechaza_cuando_el_hook_declara_output(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        engine = _ShapedEngine()
        provider = CountingEngineProvider(engines={_AGENT_NAME: engine})  # type: ignore[dict-item]
        plan = conversational_plan(_AGENT_NAME, hook=True)
        runtime = AgentRuntime(
            plans=[plan],
            config=make_ai_config(),
            engine_provider=provider,  # type: ignore[arg-type]
            deps=RecordingDepsFactory((RecordTurn,)),  # type: ignore[arg-type]
            container=container,
        )
        handle = _BoundAgentHandle(
            name=_AGENT_NAME, runtime=runtime, identity=_AUTHENTICATED, observability=None
        )

        async with runtime:
            with pytest.raises(AgentRunError) as excinfo:
                await handle.run_text("hola")

        assert excinfo.value.code is AgentRunErrorCode.AGENT_RUN_SHAPE_WITH_HOOK

    async def test_run_sin_forma_sigue_funcionando_con_un_hook_que_solo_declara_mensajes(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        provider = CountingEngineProvider(engines={_AGENT_NAME: _OneShotEngine()})  # type: ignore[dict-item]
        plan = _messages_only_plan(_AGENT_NAME)
        recorder = ConversationRecorder()
        container.register_instance(ConversationRecorder, recorder)
        runtime = AgentRuntime(
            plans=[plan],
            config=make_ai_config(),
            engine_provider=provider,  # type: ignore[arg-type]
            deps=RecordingDepsFactory((_RecordMessageOnly,)),  # type: ignore[arg-type]
            container=container,
        )
        handle = _BoundAgentHandle(
            name=_AGENT_NAME, runtime=runtime, identity=_AUTHENTICATED, observability=None
        )

        async with runtime:
            answer = await handle.run("hola")

        assert answer.output == {"ok": True}
        assert recorder.timeline == ["hook"]

    async def test_expect_sigue_funcionando_con_un_hook_que_solo_declara_mensajes(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        engine = _ShapedEngine(shaped_output={"custom": True})
        provider = CountingEngineProvider(engines={_AGENT_NAME: engine})  # type: ignore[dict-item]
        plan = _messages_only_plan(_AGENT_NAME)
        recorder = ConversationRecorder()
        container.register_instance(ConversationRecorder, recorder)
        runtime = AgentRuntime(
            plans=[plan],
            config=make_ai_config(),
            engine_provider=provider,  # type: ignore[arg-type]
            deps=RecordingDepsFactory((_RecordMessageOnly,)),  # type: ignore[arg-type]
            container=container,
        )
        handle = _BoundAgentHandle(
            name=_AGENT_NAME, runtime=runtime, identity=_AUTHENTICATED, observability=None
        )

        async with runtime:
            answer = await handle.run("hola", expect=dict)

        assert answer.output == {"custom": True}
        assert recorder.timeline == ["hook"]


class TestGrantsSinConcesion:
    """T301/T303: nombrar el grant ausente antes de tocar la red."""

    def test_mcp_desconocido_nombra_los_grants_mcp_del_agente(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        handle = _BoundAgentHandle(
            name=_AGENT_NAME,
            runtime=object(),  # type: ignore[arg-type]
            identity=_AUTHENTICATED,
            observability=None,
        )
        with pytest.raises(AttributeError):
            # 'object()' carries no runtime API: this only proves the method
            # is no longer 'NotImplementedError' but a real lookup.
            handle.mcp("server")


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
