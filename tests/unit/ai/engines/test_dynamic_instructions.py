"""The artifact's ``dynamic_instructions`` on the engine (011/L18).

Covers what the two moments of the feature promise: the factory runs once, at
build, and what it returns is checked there (AC13, AC14); the provider runs
once per **model request** — measured against a run that calls a tool, which
is two requests and therefore two provider calls — against a request struct
built from a fail-closed read of the dependency bundle (AC15), and a provider
that raises ends the run with a fixed message that carries none of its text
(AC16). The shape of the struct
itself is pinned here too, by inspecting the type (AC11).

Everything here runs against a ``FunctionModel``: no network, no credentials.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Sequence
from typing import Any

import msgspec
import pytest
from msgspec import structs
from pydantic_ai.messages import ModelMessage, ModelResponse, ToolCallPart, ToolReturnPart
from pydantic_ai.models import Model
from pydantic_ai.models.function import AgentInfo, DeltaToolCall, DeltaToolCalls, FunctionModel
from pydantic_ai.toolsets import FunctionToolset

from loom.ai.abc import (
    AgentEngine,
    DepsFactory,
    InstructionsProvider,
    InstructionsRequest,
    ToolsetContext,
)
from loom.ai.compiler._plan import (
    AgentPlan,
    CompiledDynamicInstructions,
    CompiledPythonCapability,
)
from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider
from loom.ai.engines.pydantic_ai._instructions import INSTRUCTIONS_FAILED_MESSAGE
from loom.ai.errors import AgentCompilationError, AgentErrorCode, AgentRunError, AgentRunErrorCode
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from tests.helpers.pydantic_ai_engine import STRICT_SCHEMA, NullDeps, make_plan

_IDENTITY = Identity(subject="clerk-1", roles=("clerk",), mechanism="test")
_PROMPT = "what happened to order 42?"
_ANSWER = '{"answer": "done"}'
_TOOL = "lookup_incident"


class _Deps:
    """A well-formed dependency bundle: a verified caller and a container."""

    def __init__(self, identity: Identity) -> None:
        self.identity = identity
        self.container = LoomContainer()


class _DepsFactory:
    """Per-invocation factory producing the bundle the instructions path reads."""

    def build(self, identity: Identity, container: LoomContainer) -> object:
        """Return the bundle carrying the run's verified caller."""
        del container
        return _Deps(identity)


def _model(seen: list[str | None] | None = None) -> Model:
    """A model answering a fixed payload and recording the instructions it was sent."""

    def observe(messages: Sequence[ModelMessage]) -> None:
        if seen is not None:
            seen.extend(getattr(message, "instructions", None) for message in messages)

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        observe(messages)
        part = ToolCallPart(tool_name=info.output_tools[0].name, args=_ANSWER)
        return ModelResponse(parts=[part])

    async def stream(
        messages: list[ModelMessage], info: AgentInfo
    ) -> AsyncIterator[DeltaToolCalls]:
        observe(messages)
        name = info.output_tools[0].name
        yield {0: DeltaToolCall(name=name, json_args=_ANSWER, tool_call_id="call")}

    return FunctionModel(respond, stream_function=stream)


def _tool_capability() -> CompiledPythonCapability:
    """A grant publishing the one tool :func:`_tool_calling_model` calls."""

    def build(context: ToolsetContext) -> FunctionToolset[Any]:
        del context
        toolset: FunctionToolset[Any] = FunctionToolset()
        toolset.add_function(lambda: "INC-1 is open", name=_TOOL)
        return toolset

    return CompiledPythonCapability(factory_ref="tests:build", factory=build)


def _tool_calling_model(requests: list[int]) -> Model:
    """A model that calls one tool on its first request and answers on its second.

    The normal shape of a run with capabilities, and the reason the provider is
    not called once per run: every one of these requests rebuilds the
    instructions. ``requests`` receives one entry per model request.
    """

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        requests.append(len(requests))
        if not any(isinstance(part, ToolReturnPart) for part in _parts(messages)):
            return ModelResponse(parts=[ToolCallPart(tool_name=_TOOL, args={}, tool_call_id="c1")])
        return ModelResponse(
            parts=[ToolCallPart(tool_name=info.output_tools[0].name, args=_ANSWER)]
        )

    return FunctionModel(respond)


def _parts(messages: Sequence[ModelMessage]) -> list[Any]:
    """Flatten every part of a request conversation."""
    return [part for message in messages for part in getattr(message, "parts", ())]


def _plan_with(
    factory: Callable[..., Any],
    *,
    capabilities: tuple[CompiledPythonCapability, ...] = (),
    **params: Any,
) -> AgentPlan:
    """A compiled plan carrying *factory*, and the capabilities the run may use."""
    return structs.replace(
        make_plan(schema=STRICT_SCHEMA),
        dynamic_instructions=CompiledDynamicInstructions(
            factory_ref="tests:factory", factory=factory, params=params
        ),
        capabilities=capabilities,
    )


def _build(plan: AgentPlan, model: Model, *, deps: DepsFactory | None = None) -> AgentEngine:
    """Build the real adapter over a scripted model."""
    provider = PydanticAIEngineProvider(model_resolver=lambda target: model)
    return provider.create_engine(
        plan, deps=deps if deps is not None else _DepsFactory(), container=LoomContainer()
    )


async def _run(engine: AgentEngine, prompt: str = _PROMPT) -> object:
    """Run one prompt through the engine as the fixed caller."""
    return await engine.run(prompt, identity=_IDENTITY)


class TestFormaDeLaPeticion:
    """AC11: the request is a frozen struct of exactly four names."""

    def test_la_peticion_expone_exactamente_los_cuatro_nombres_declarados(self) -> None:
        """The list is fixed: what reaches prompt-building code is a security decision."""
        names = tuple(field.name for field in msgspec.structs.fields(InstructionsRequest))

        assert names == ("agent", "prompt", "subject", "mechanism")

    @pytest.mark.parametrize("name", ["container", "invoker", "identity", "conversation_id"])
    def test_la_peticion_no_alcanza_el_bundle_de_dependencias(self, name: str) -> None:
        """Asserted on the type, not on a docstring: no attribute, no route."""
        request = InstructionsRequest(agent="a", prompt="p", subject="s", mechanism="m")

        assert not hasattr(request, name)

    def test_la_peticion_es_inmutable(self) -> None:
        """A provider cannot rewrite the subject it was told about."""
        request = InstructionsRequest(agent="a", prompt="p", subject="s", mechanism="m")

        with pytest.raises(AttributeError):
            request.subject = "someone-else"  # type: ignore[misc]


class TestConstruccion:
    """AC13 and AC14: the factory runs once, and what it returns is checked."""

    @pytest.mark.asyncio
    async def test_la_factoria_se_llama_una_vez_para_n_corridas(self) -> None:
        """The factory is build-time work; three runs must not pay for it three times."""
        builds: list[ToolsetContext] = []
        requests: list[int] = []

        def factory(context: ToolsetContext) -> InstructionsProvider:
            builds.append(context)
            return lambda request: "extra"

        engine = _build(
            _plan_with(factory, capabilities=(_tool_capability(),)), _tool_calling_model(requests)
        )
        for _ in range(3):
            await _run(engine)

        assert len(builds) == 1
        assert len(requests) == 6

    @pytest.mark.asyncio
    async def test_el_provider_se_llama_una_vez_por_peticion_al_modelo_no_por_corrida(
        self,
    ) -> None:
        """AC13: the engine rebuilds the instructions before every model request.

        One tool call already makes the run cost two requests, and the provider
        is called on both — which is the normal case for any agent holding
        capabilities, not an edge. The numbers are the measured ones, so
        collapsing the two calls into one would fail here.
        """
        requests: list[int] = []
        calls: list[str] = []

        def factory(context: ToolsetContext) -> InstructionsProvider:
            del context
            return lambda request: calls.append(request.prompt) or "extra"

        engine = _build(
            _plan_with(factory, capabilities=(_tool_capability(),)), _tool_calling_model(requests)
        )
        await _run(engine)

        assert len(requests) == 2
        assert calls == [_PROMPT, _PROMPT]

    def test_la_factoria_recibe_los_params_declarados(self) -> None:
        """``params`` are the artifact's settings, splatted as keyword arguments."""
        received: list[str] = []

        def factory(context: ToolsetContext, *, locale: str = "en") -> InstructionsProvider:
            del context
            received.append(locale)
            return lambda request: None

        _build(_plan_with(factory, locale="es"), _model())

        assert received == ["es"]

    def test_falla_en_construccion_con_su_propio_codigo_cuando_el_provider_es_corrutina(
        self,
    ) -> None:
        """An awaited provider would enter the prompt path silently (AC14)."""

        def factory(context: ToolsetContext) -> Any:
            del context

            async def provide(request: InstructionsRequest) -> str | None:
                return request.prompt

            return provide

        with pytest.raises(AgentCompilationError) as exc:
            _build(_plan_with(factory), _model())

        assert [issue.code for issue in exc.value.issues] == [
            AgentErrorCode.DYNAMIC_INSTRUCTIONS_PROVIDER_COROUTINE
        ]

    def test_falla_en_construccion_cuando_la_factoria_no_devuelve_algo_llamable(self) -> None:
        """A provider is called on every model request, so it is refused before any."""
        with pytest.raises(AgentCompilationError) as exc:
            _build(_plan_with(lambda context: 42), _model())

        assert [issue.code for issue in exc.value.issues] == [
            AgentErrorCode.DYNAMIC_INSTRUCTIONS_NOT_CALLABLE
        ]

    def test_falla_en_construccion_reportando_la_clase_y_no_el_texto_cuando_la_factoria_revienta(
        self,
    ) -> None:
        """The message could echo a ``params`` value, so only the class is named."""
        secret = "tarjeta-4242"

        def factory(context: ToolsetContext) -> InstructionsProvider:
            del context
            raise RuntimeError(secret)

        with pytest.raises(AgentCompilationError) as exc:
            _build(_plan_with(factory), _model())

        issue = exc.value.issues[0]
        assert issue.code is AgentErrorCode.DYNAMIC_INSTRUCTIONS_FACTORY_FAILED
        assert "RuntimeError" in issue.message
        assert secret not in issue.message

    @pytest.mark.asyncio
    async def test_el_prompt_es_solo_el_literal_cuando_el_artefacto_no_declara_el_bloque(
        self,
    ) -> None:
        """The field is optional and additive: an artifact without it is unchanged."""
        seen: list[str | None] = []

        await _run(_build(make_plan(schema=STRICT_SCHEMA), _model(seen)))

        assert next(item for item in seen if item) == "answer the question"


class TestCorrida:
    """AC15 and AC16: what the provider is given, and what a failing one costs."""

    @pytest.mark.asyncio
    async def test_el_provider_recibe_el_sujeto_y_el_mecanismo_verificados(self) -> None:
        """The identity comes from the run's bundle, never from the prompt."""
        seen: list[InstructionsRequest] = []

        def factory(context: ToolsetContext) -> InstructionsProvider:
            del context
            return lambda request: seen.append(request) or None

        await _run(_build(_plan_with(factory), _model()))

        assert [(item.subject, item.mechanism) for item in seen] == [("clerk-1", "test")]

    @pytest.mark.asyncio
    async def test_el_provider_recibe_el_prompt_y_el_nombre_del_agente(self) -> None:
        """The prompt is what makes per-request text possible at all."""
        seen: list[InstructionsRequest] = []

        def factory(context: ToolsetContext) -> InstructionsProvider:
            del context
            return lambda request: seen.append(request) or None

        await _run(_build(_plan_with(factory), _model()))

        assert [(item.agent, item.prompt) for item in seen] == [("contract", _PROMPT)]

    @pytest.mark.asyncio
    async def test_la_corrida_falla_como_no_autorizada_cuando_el_bundle_no_lleva_identidad(
        self,
    ) -> None:
        """A bundle without a verified caller fails closed, before any tool runs (AC15)."""

        def factory(context: ToolsetContext) -> InstructionsProvider:
            del context
            return lambda request: "extra"

        engine = _build(_plan_with(factory), _model(), deps=NullDeps())

        with pytest.raises(AgentRunError) as exc:
            await _run(engine)

        assert exc.value.code is AgentRunErrorCode.UNAUTHORIZED

    @pytest.mark.asyncio
    async def test_la_corrida_termina_con_mensaje_fijo_cuando_el_provider_revienta(self) -> None:
        """The application's exception text never reaches the caller (AC16)."""
        secret = "expediente-77"

        def factory(context: ToolsetContext) -> InstructionsProvider:
            del context

            def provide(request: InstructionsRequest) -> str | None:
                raise RuntimeError(f"{secret}: {request.prompt}")

            return provide

        with pytest.raises(AgentRunError) as exc:
            await _run(_build(_plan_with(factory), _model()))

        assert exc.value.code is AgentRunErrorCode.INSTRUCTIONS_FAILED
        assert str(exc.value) == INSTRUCTIONS_FAILED_MESSAGE
        assert secret not in str(exc.value)

    @pytest.mark.asyncio
    async def test_el_literal_compone_primero_y_el_texto_dinamico_despues(self) -> None:
        """The composition order the artifact promises, read off what the model was sent."""
        seen: list[str | None] = []

        def factory(context: ToolsetContext) -> InstructionsProvider:
            del context
            return lambda request: f"CHECKLIST for {request.prompt}"

        await _run(_build(_plan_with(factory), _model(seen)))

        composed = next(item for item in seen if item)
        assert composed.startswith("answer the question")
        assert composed.endswith(f"CHECKLIST for {_PROMPT}")

    @pytest.mark.asyncio
    async def test_el_literal_viaja_solo_cuando_el_provider_no_aporta_nada(self) -> None:
        """``None`` contributes nothing; it does not blank the literal."""
        seen: list[str | None] = []

        def factory(context: ToolsetContext) -> InstructionsProvider:
            del context
            return lambda request: None

        await _run(_build(_plan_with(factory), _model(seen)))

        assert next(item for item in seen if item) == "answer the question"
