"""``dynamic_instructions`` end to end, on the case that motivated it (011/L18).

The whole path runs: a real ``.agent.yaml`` is decoded, compiled by the real
compiler, served by the real ``AgentRuntime`` over the real pydantic-ai
adapter. Only the model is scripted, and it records the instructions it was
sent — which is the one place the composed prompt is observable from outside.

The main agent declares **no capabilities**, which is the point. Contributing
instructions used to require declaring a ``kind: python`` toolset, so an agent
that wanted per-request text and no tools had no route at all (AC10). And the
two runs share one subject and differ only in their prompt, because that is
what makes the text differ: without the prompt the provider would be a
constant per caller and a per-request checklist would be impossible (AC12).

A second artifact, identical but holding one tool, is what makes the real cost
of the provider observable: a run that calls a tool makes two model requests,
and the engine rebuilds the instructions before each one, so the provider runs
twice for a single run.

The checklist catalogue is resolved from the application container, at build,
by the factory — so the provider itself reads nothing and does no I/O, as its
contract requires.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from pathlib import Path
from typing import Any

import pytest
from pydantic_ai.messages import ModelMessage, ModelResponse, ToolCallPart, ToolReturnPart
from pydantic_ai.models import Model
from pydantic_ai.models.function import AgentInfo, DeltaToolCall, DeltaToolCalls, FunctionModel
from pydantic_ai.toolsets import FunctionToolset

from loom.ai.abc import InstructionsProvider, InstructionsRequest, ToolsetContext
from loom.ai.compiler import AgentCompiler, AgentPlan
from loom.ai.declarative import load_specs
from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider
from loom.ai.runtime import AgentRuntime
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from loom.core.use_case.registry import UseCaseRegistry
from tests.integration.ai.conftest import CapabilityDepsFactory, make_ai_config

pytestmark = pytest.mark.asyncio

_AGENT = "incident-triage"
_CALLER = Identity(subject="oncall-7", roles=("oncall",), mechanism="test")
_ANSWER = '{"answer": "acknowledged"}'

_ARTIFACT = """
spec_version: 1
name: incident-triage
description: Investigates production incidents and proposes the next remediation step.
instructions: >-
  Investigate the reported incident and propose the next remediation step.
dynamic_instructions:
  factory: tests.integration.ai.test_dynamic_instructions:build_checklist
  params:
    locale: en
output:
  kind: json_schema
  schema:
    type: object
    additionalProperties: false
    required:
      - answer
    properties:
      answer:
        type: string
"""
"""The artifact of the reported case: per-request instructions, and no capabilities."""

_ARTIFACT_WITH_TOOL = (
    _ARTIFACT.replace("name: incident-triage", "name: incident-triage-tooled")
    + """
capabilities:
  - kind: python
    factory: tests.integration.ai.test_dynamic_instructions:build_lookup_toolset
"""
)
"""The same artifact holding one tool, which is what makes a run cost two model requests."""

_TOOLED_AGENT = "incident-triage-tooled"
_TOOL = "lookup_incident"


class ChecklistCatalog:
    """Application service holding the checklist of each kind of incident.

    Args:
        checklists: Steps to demand, keyed by the word that selects them.
    """

    def __init__(self, checklists: dict[str, str]) -> None:
        self._checklists = checklists
        self.renders = 0

    def render(self, prompt: str, *, locale: str) -> str | None:
        """Return the checklist the prompt calls for, or ``None`` for none.

        Args:
            prompt: What the caller asked, used only to choose a checklist.
            locale: Language tag the checklist is labelled with.

        Returns:
            The checklist text, or ``None`` when no keyword matches.
        """
        self.renders += 1
        for keyword, checklist in self._checklists.items():
            if keyword in prompt:
                return f"[{locale}] {checklist}"
        return None


def build_checklist(context: ToolsetContext, *, locale: str = "en") -> InstructionsProvider:
    """Build the per-request checklist provider once, at start-up.

    Args:
        context: Build-time context; the catalogue is resolved from it here so
            the provider itself reads nothing.
        locale: Language tag the rendered checklist is labelled with.

    Returns:
        The provider called once per model request.
    """
    catalog = context.container.resolve(ChecklistCatalog)

    def provide(request: InstructionsRequest) -> str | None:
        return catalog.render(request.prompt, locale=locale)

    return provide


def build_lookup_toolset(context: ToolsetContext) -> FunctionToolset[Any]:
    """Publish the one tool the tooled artifact grants.

    Args:
        context: Build-time context; this toolset reads nothing from it.

    Returns:
        The engine toolset holding the single lookup tool.
    """
    del context
    toolset: FunctionToolset[Any] = FunctionToolset()
    toolset.add_function(lambda: "INC-1 is open", name=_TOOL)
    return toolset


class _ToolCallingModel:
    """A model that calls the granted tool once, then answers.

    The ordinary shape of a run with capabilities, and the one that makes the
    per-request cost of the instructions observable: two model requests, so the
    engine builds the instructions twice.

    Attributes:
        requests: One entry per request the engine made to the model.
    """

    def __init__(self) -> None:
        self.requests: list[int] = []

    def as_model(self) -> Model:
        """Return the ``FunctionModel`` the engine drives, in both run modes."""
        return FunctionModel(self._respond, stream_function=self._stream)

    def _respond(self, messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        name, arguments = self._next_call(messages, info)
        part = ToolCallPart(tool_name=name, args=arguments, tool_call_id="c1")
        return ModelResponse(parts=[part])

    async def _stream(
        self, messages: list[ModelMessage], info: AgentInfo
    ) -> AsyncIterator[DeltaToolCalls]:
        name, arguments = self._next_call(messages, info)
        yield {0: DeltaToolCall(name=name, json_args=arguments, tool_call_id="c1")}

    def _next_call(self, messages: Sequence[ModelMessage], info: AgentInfo) -> tuple[str, str]:
        """Record the request and choose between calling the tool and answering."""
        self.requests.append(len(self.requests))
        answered = any(
            isinstance(part, ToolReturnPart)
            for message in messages
            for part in getattr(message, "parts", ())
        )
        if not answered:
            return _TOOL, "{}"
        return info.output_tools[0].name, _ANSWER


class _RecordingModel:
    """A model answering a fixed payload and keeping the instructions it was sent."""

    def __init__(self) -> None:
        self.instructions: list[str] = []

    def as_model(self) -> Model:
        """Return the ``FunctionModel`` the engine drives, in both run modes."""
        return FunctionModel(self._respond, stream_function=self._stream)

    def _observe(self, messages: Sequence[ModelMessage]) -> None:
        for message in messages:
            composed = getattr(message, "instructions", None)
            if composed:
                self.instructions.append(composed)

    def _respond(self, messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        self._observe(messages)
        part = ToolCallPart(tool_name=info.output_tools[0].name, args=_ANSWER)
        return ModelResponse(parts=[part])

    async def _stream(
        self, messages: list[ModelMessage], info: AgentInfo
    ) -> AsyncIterator[DeltaToolCalls]:
        self._observe(messages)
        name = info.output_tools[0].name
        yield {0: DeltaToolCall(name=name, json_args=_ANSWER, tool_call_id="call")}


def _compile_artifact(tmp_path: Path, artifact: str = _ARTIFACT) -> AgentPlan:
    """Decode and compile the artifact exactly as a deployment would."""
    path = tmp_path / "incident-triage" / "agent.yaml"
    path.parent.mkdir(parents=True)
    path.write_text(artifact, encoding="utf-8")
    decoded = load_specs(["*/agent.yaml"], root=tmp_path)
    compiler = AgentCompiler(
        config=make_ai_config(),
        registry=UseCaseRegistry({}, {}),
        supported_kinds=frozenset({"usecase", "python"}),
    )
    return compiler.compile_all(decoded)[0]


def _runtime(
    plan: AgentPlan, model: _RecordingModel | _ToolCallingModel, catalog: ChecklistCatalog
) -> AgentRuntime:
    """Assemble the runtime over the real engine and a scripted model."""
    container = LoomContainer()
    container.register_instance(ChecklistCatalog, catalog)
    return AgentRuntime(
        plans=[plan],
        config=make_ai_config(),
        engine_provider=PydanticAIEngineProvider(model_resolver=lambda target: model.as_model()),
        deps=CapabilityDepsFactory(),  # type: ignore[arg-type]
        container=container,
    )


def _catalog() -> ChecklistCatalog:
    """The catalogue the artifact's factory resolves at start-up."""
    return ChecklistCatalog(
        {
            "database": "check replication lag, then the slow query log.",
            "network": "check the load balancer pool, then the egress quota.",
        }
    )


async def test_el_agente_sin_capacidades_recibe_una_checklist_por_peticion(
    tmp_path: Path,
) -> None:
    """AC10: per-request text reaches the model, with the literal composing first."""
    model = _RecordingModel()
    plan = _compile_artifact(tmp_path)

    async with _runtime(plan, model, _catalog()) as runtime:
        await runtime.run(_AGENT, "the database is timing out", identity=_CALLER)

    assert plan.capabilities == ()
    composed = model.instructions[0]
    assert composed.startswith("Investigate the reported incident")
    assert composed.endswith("[en] check replication lag, then the slow query log.")


async def test_dos_corridas_del_mismo_sujeto_con_prompts_distintos_dan_textos_distintos(
    tmp_path: Path,
) -> None:
    """AC12: the prompt is what makes the feature earn its place.

    Without it the other three names vary only by caller, so these two runs
    would be identical by construction.
    """
    model = _RecordingModel()

    async with _runtime(_compile_artifact(tmp_path), model, _catalog()) as runtime:
        await runtime.run(_AGENT, "the database is timing out", identity=_CALLER)
        await runtime.run(_AGENT, "the network is dropping packets", identity=_CALLER)

    first, second = model.instructions
    assert first != second
    assert first.endswith("[en] check replication lag, then the slow query log.")
    assert second.endswith("[en] check the load balancer pool, then the egress quota.")


async def test_la_factoria_corre_una_vez_y_el_provider_una_por_peticion_al_modelo(
    tmp_path: Path,
) -> None:
    """AC13 over the whole path: start-up work is not paid per request.

    Three runs of an agent with no tools answer in one model request each, so
    the provider is called three times; the factory is called once whatever the
    number of runs, which is the property this pins.
    """
    model = _RecordingModel()
    catalog = _catalog()

    async with _runtime(_compile_artifact(tmp_path), model, catalog) as runtime:
        for prompt in ("the database is slow", "the network is slow", "something else"):
            await runtime.run(_AGENT, prompt, identity=_CALLER)

    assert len(model.instructions) == 3
    assert catalog.renders == 3


async def test_el_provider_corre_una_vez_por_peticion_al_modelo_no_por_corrida(
    tmp_path: Path,
) -> None:
    """The other half of AC13, and the one a run with tools makes visible.

    One tool call makes a single run cost two model requests, and the engine
    rebuilds the instructions before each of them — so the provider is called
    twice for one run. That is the normal case for any agent with capabilities,
    not an edge, and the number is the measured one.
    """
    model = _ToolCallingModel()
    catalog = _catalog()
    plan = _compile_artifact(tmp_path, _ARTIFACT_WITH_TOOL)

    async with _runtime(plan, model, catalog) as runtime:
        await runtime.run(_TOOLED_AGENT, "the database is timing out", identity=_CALLER)

    assert len(model.requests) == 2
    assert catalog.renders == 2


async def test_el_literal_viaja_solo_cuando_ninguna_checklist_aplica(tmp_path: Path) -> None:
    """A provider returning ``None`` adds nothing; it never blanks the literal."""
    model = _RecordingModel()

    async with _runtime(_compile_artifact(tmp_path), model, _catalog()) as runtime:
        await runtime.run(_AGENT, "the coffee machine is down", identity=_CALLER)

    assert model.instructions[0].strip().startswith("Investigate the reported incident")
    assert "[en]" not in model.instructions[0]


async def test_el_plan_compilado_lleva_la_factoria_importada(tmp_path: Path) -> None:
    """The artifact names it; the plan carries the handle, resolved offline."""
    compiled: Any = _compile_artifact(tmp_path).dynamic_instructions

    assert compiled is not None
    assert compiled.factory is build_checklist
    assert compiled.params == {"locale": "en"}
