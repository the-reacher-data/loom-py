"""The artifact's ``output_check`` on the engine (011/L17, L20).

Covers what an artifact gains by declaring the field: the split retry budget
the check needs to be able to correct anything (AC3, AC4), the correction
itself over the streaming path the runtime really uses (AC6), the build-time
refusal of the one output mode that cannot serve a check (AC7), and the fact
that a check cannot change the answer by mutating what it is handed (AC9).

Everything here runs against a ``FunctionModel``: no network, no credentials.
The single-shot ``run`` path is deliberately not the subject — the runtime
serves every run over the stream, so an acceptance measured on ``run`` would
measure code production does not execute.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from typing import Any

import pytest
from msgspec import structs
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models import Model
from pydantic_ai.models.function import AgentInfo, DeltaToolCall, DeltaToolCalls, FunctionModel

from loom.ai.abc import AgentEvent, FinalEvent, OutputCheck, TextDeltaEvent
from loom.ai.compiler._plan import AgentPlan
from loom.ai.declarative import PolicySpec
from loom.ai.engines.pydantic_ai._spec import build_agent_spec
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.inference import InferenceTarget
from loom.core.identity import Identity
from tests.helpers.pydantic_ai_engine import STRICT_SCHEMA, build_engine, make_plan

_IDENTITY = Identity(subject="caller")
_PROMPT = "what happened?"

_REJECTED = '{"answer": "incomplete"}'
"""First attempt: satisfies the schema, breaks the rule."""

_ACCEPTED = '{"answer": "complete"}'
"""Second attempt: what the model answers after reading the rejection message."""

_CORRECTION = "say 'complete', not 'incomplete'"
"""The text a rejection hands the model; it is the check's whole output."""

_NATIVE = InferenceTarget(provider="openai", model="gpt-5.2", output_mode="native")
"""A binding whose role pins the one output mode a check cannot be served under."""


def _accept_only_complete(seen: list[Mapping[str, Any]]) -> OutputCheck:
    """A check recording every answer it saw and rejecting all but the right one."""

    def check(answer: Mapping[str, Any]) -> str | None:
        seen.append(dict(answer))
        return None if answer.get("answer") == "complete" else _CORRECTION

    return check


def _plan_with_check(
    check: OutputCheck | None,
    *,
    retries: int = 1,
    inference: InferenceTarget | None = None,
) -> AgentPlan:
    """A compiled plan for a pure-language agent carrying *check*."""
    plan = structs.replace(
        make_plan(schema=STRICT_SCHEMA),
        output_check=check,
        policies=PolicySpec(retries=retries),
    )
    if inference is None:
        return plan
    return structs.replace(plan, inference=inference)


def _tool_model(payloads: Sequence[str], seen: list[list[ModelMessage]] | None = None) -> Model:
    """A model answering *payloads* in order as output-tool calls.

    The tool output mode is what the engine resolves by itself from the plan's
    ``output_schema`` when no mode is pinned, so this is the default path.
    """
    answers = iter(payloads)

    def observe(messages: list[ModelMessage]) -> None:
        if seen is not None:
            seen.append(list(messages))

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        observe(messages)
        part = ToolCallPart(tool_name=info.output_tools[0].name, args=next(answers))
        return ModelResponse(parts=[part])

    async def stream(
        messages: list[ModelMessage], info: AgentInfo
    ) -> AsyncIterator[DeltaToolCalls]:
        observe(messages)
        call = DeltaToolCall(
            name=info.output_tools[0].name, json_args=next(answers), tool_call_id="call"
        )
        yield {0: call}

    return FunctionModel(respond, stream_function=stream)


def _text_model(payloads: Sequence[str]) -> Model:
    """A model answering *payloads* in order as plain text, as a native answer arrives."""
    answers = iter(payloads)

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[TextPart(content=next(answers))])

    async def stream(messages: list[ModelMessage], info: AgentInfo) -> AsyncIterator[str]:
        yield next(answers)

    return FunctionModel(respond, stream_function=stream)


async def _stream_events(plan: AgentPlan, model: Model) -> list[AgentEvent]:
    """Drain the streaming path — the one the runtime serves every run over."""
    engine = build_engine(plan, model)
    async with engine.run_stream(_PROMPT, identity=_IDENTITY, conversation=None) as events:
        return [event async for event in events]


def _single_final(events: Sequence[AgentEvent]) -> FinalEvent:
    """Return the run's one terminal event, failing loudly on any other outcome."""
    finals = [event for event in events if isinstance(event, FinalEvent)]
    assert len(finals) == 1, [type(event).__name__ for event in events]
    return finals[0]


def _answer(final: FinalEvent) -> object:
    """Read the one declared field off the struct the plan's decoder built.

    The decoded type is generated from the artifact's schema at compile time,
    so it has no static name and the read goes through the instance's fields.
    """
    output: Any = final.output
    return output.answer


def _text_deltas(events: Sequence[AgentEvent]) -> list[str]:
    """Every text delta the consumer saw, in order."""
    return [event.text for event in events if isinstance(event, TextDeltaEvent)]


def _rendered(messages: Sequence[ModelMessage]) -> str:
    """Flatten a request conversation to searchable text."""
    return "\n".join(
        str(getattr(part, "content", part)) for message in messages for part in message.parts
    )


class TestPresupuestoDeReintento:
    """``retries`` reaches the engine split in two, and the check floors one axis."""

    @pytest.mark.parametrize("declared", [0, 2, 5], ids=["zero", "default", "high"])
    def test_los_dos_ejes_valen_lo_declarado_cuando_no_hay_check(self, declared: int) -> None:
        """Without a check the projection is the engine's own reading of a bare int.

        This is the compatibility claim of the change: every artifact that
        exists today keeps the behaviour it had.
        """
        spec = build_agent_spec(_plan_with_check(None, retries=declared))

        assert spec.retries == {"tools": declared, "output": declared}

    def test_el_eje_de_salida_sube_a_uno_cuando_hay_check_y_se_declaro_cero(self) -> None:
        """A check with no attempt left could only fail runs, never correct one."""
        spec = build_agent_spec(_plan_with_check(_accept_only_complete([]), retries=0))

        assert spec.retries == {"tools": 0, "output": 1}

    @pytest.mark.parametrize("declared", [2, 5], ids=["default", "high"])
    def test_el_eje_de_salida_respeta_lo_declarado_cuando_supera_el_suelo(
        self, declared: int
    ) -> None:
        """The floor raises a zero; it never lowers what the artifact asked for."""
        spec = build_agent_spec(_plan_with_check(_accept_only_complete([]), retries=declared))

        assert spec.retries == {"tools": declared, "output": declared}


class TestCorreccionEnStreaming:
    """A rejection buys the model another attempt, over the runtime's own path."""

    async def test_el_modelo_corrige_y_la_corrida_termina_bien_cuando_el_check_rechaza(
        self,
    ) -> None:
        """Two model requests, one valid final event, and the corrected answer."""
        seen: list[Mapping[str, Any]] = []
        plan = _plan_with_check(_accept_only_complete(seen))

        events = await _stream_events(plan, _tool_model([_REJECTED, _ACCEPTED]))

        final = _single_final(events)
        assert [answer["answer"] for answer in seen] == ["incomplete", "complete"]
        assert final.usage.requests == 2
        assert _answer(final) == "complete"

    async def test_el_consumidor_no_ve_la_respuesta_rechazada_en_modo_tool(self) -> None:
        """Loom projects only text parts as deltas, and a tool answer is not one.

        This is why an engine-level retry does not break loom's rule that a run
        is never replayed once a delta has reached the caller: in the tool
        output mode a rejected attempt produces no consumer-visible output.
        """
        plan = _plan_with_check(_accept_only_complete([]))

        events = await _stream_events(plan, _tool_model([_REJECTED, _ACCEPTED]))

        assert _text_deltas(events) == []

    async def test_el_mensaje_del_check_llega_al_modelo_cuando_rechaza(self) -> None:
        """The rejection is an instruction, so it must reach the next request."""
        conversations: list[list[ModelMessage]] = []
        plan = _plan_with_check(_accept_only_complete([]))

        await _stream_events(plan, _tool_model([_REJECTED, _ACCEPTED], conversations))

        assert _CORRECTION in _rendered(conversations[-1])

    async def test_la_corrida_falla_cuando_el_check_rechaza_cada_intento(self) -> None:
        """The budget is finite: a check that never accepts ends the run, it does not loop."""
        seen: list[Mapping[str, Any]] = []
        plan = _plan_with_check(_accept_only_complete(seen), retries=1)

        events = await _stream_events(plan, _tool_model([_REJECTED, _REJECTED, _REJECTED]))

        assert [type(event).__name__ for event in events] == ["ErrorEvent"]
        assert len(seen) == 2


class TestModoNativo:
    """The one binding a check cannot be served under is refused at build (AC7).

    Measured before it was decided: with ``output_mode: native`` the provider
    delivers the structured answer as text parts, loom projects text parts as
    deltas and the engine retries inside one run, so a rejection would show the
    caller the rejected payload and then the accepted one — the same answer
    twice, against loom's rule that a run is never replayed once a delta has
    reached the caller. Neither half may be overridden: the mode is deployment
    configuration and the check is the artifact's contract. So the mismatch is
    reported the way every other artifact-versus-deployment mismatch is, at
    build, with a code of its own.
    """

    def test_falla_en_construccion_con_su_propio_codigo_cuando_el_modo_es_nativo(self) -> None:
        """Both halves are known in ``create_engine``, so nothing is deferred to a run."""
        plan = _plan_with_check(_accept_only_complete([]), inference=_NATIVE)

        with pytest.raises(AgentCompilationError) as excinfo:
            build_engine(plan, _text_model([_ACCEPTED]))

        (issue,) = excinfo.value.issues
        assert issue.code is AgentErrorCode.OUTPUT_CHECK_NATIVE_MODE_UNSUPPORTED
        assert issue.component == plan.name
        assert issue.field == "output_check"

    def test_el_mensaje_nombra_las_dos_salidas_y_el_motivo(self) -> None:
        """An operator reads it without the spec: why it fails, and the two ways out."""
        plan = _plan_with_check(_accept_only_complete([]), inference=_NATIVE)

        with pytest.raises(AgentCompilationError) as excinfo:
            build_engine(plan, _text_model([_ACCEPTED]))

        message = excinfo.value.issues[0].message
        assert "twice" in message
        assert "output_mode: tool" in message
        assert "remove the artifact's output_check" in message

    def test_el_modo_nativo_se_construye_cuando_el_artefacto_no_declara_check(self) -> None:
        """The refusal is the pair, never the mode: a native binding alone is fine."""
        plan = _plan_with_check(None, inference=_NATIVE)

        assert build_engine(plan, _text_model([_ACCEPTED])) is not None

    def test_el_check_se_construye_cuando_el_modo_esta_fijado_a_tool(self) -> None:
        """The tool mode duplicates nothing, so a check under it is served normally."""
        pinned = InferenceTarget(provider="openai", model="gpt-5.2", output_mode="tool")
        plan = _plan_with_check(_accept_only_complete([]), inference=pinned)

        assert build_engine(plan, _tool_model([_ACCEPTED])) is not None


class TestPurezaDelCheck:
    """A check is a predicate; nothing it does to its argument reaches the answer."""

    async def test_mutar_el_mapping_recibido_no_altera_la_salida_decodificada(self) -> None:
        """Loom decodes the raw messages, so the check's mutation is invisible.

        The mutation is deliberately one a strict decode would refuse — the
        schema forbids an unknown field — so the run succeeding is proof that
        the decode never reads the mutated object.
        """
        received: list[Mapping[str, Any]] = []

        def vandalising_check(answer: Mapping[str, Any]) -> str | None:
            assert isinstance(answer, dict)
            answer["answer"] = "tampered"
            answer["injected"] = True
            received.append(answer)
            return None

        plan = _plan_with_check(vandalising_check)

        events = await _stream_events(plan, _tool_model([_ACCEPTED]))

        assert received[0]["answer"] == "tampered"
        assert _answer(_single_final(events)) == "complete"
