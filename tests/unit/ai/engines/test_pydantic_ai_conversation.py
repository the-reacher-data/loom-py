"""Conversation memory through the pydantic-ai adapter (AC11, AC12).

The engine decodes the history loom hands it, replays it to the model before
the new prompt, forwards loom's ``conversation_id`` so every new message is
stamped with it, and returns only this run's messages. A history that does
not validate fails before the model is called, with nothing spent.
"""

from __future__ import annotations

import pytest
from pydantic_ai.messages import (
    ModelMessage,
    ModelMessagesTypeAdapter,
    ModelRequest,
    ModelResponse,
    TextPart,
    UserPromptPart,
)

from loom.ai.abc import AgentEngine, AgentEvent, Conversation, ErrorEvent, FinalEvent
from loom.ai.errors import CONVERSATION_LOAD_FAILED_MESSAGE, AgentRunError, AgentRunErrorCode
from loom.core.identity import Identity
from tests.helpers.pydantic_ai_engine import (
    build_engine,
    encode,
    flaky_model,
    make_plan,
    recording_model,
)

_IDENTITY = Identity(subject="caller")
_PROMPT = "and now?"
_ANSWER = {"answer": "still fine"}
_CONVERSATION_ID = "c-42"

# An output-tool answer is closed by pydantic-ai with a tool-return request,
# so one turn over this plan is request, response, tool-return request.
_TURN_KINDS = ("request", "response", "request")


def _history() -> bytes:
    prior: list[ModelMessage] = [
        ModelRequest(parts=[UserPromptPart(content="how is the cluster?")]),
        ModelResponse(parts=[TextPart(content="fine")]),
    ]
    return ModelMessagesTypeAdapter.dump_json(prior)


def _conversation(history: bytes | None = None) -> Conversation:
    return Conversation(conversation_id=_CONVERSATION_ID, history=history)


def _engine(seen: list[list[ModelMessage]], *, retries: int = 0, failures: int = 0) -> AgentEngine:
    payload = encode(_ANSWER)
    model = (
        flaky_model(failures, payload, seen=seen) if failures else recording_model(payload, seen)
    )
    return build_engine(make_plan(retries=retries), model)


def _decode(raw: bytes | None) -> list[ModelMessage]:
    assert raw is not None
    return ModelMessagesTypeAdapter.validate_json(raw)


def _prompts(messages: list[ModelMessage]) -> list[str]:
    return [
        str(part.content)
        for message in messages
        if isinstance(message, ModelRequest)
        for part in message.parts
        if isinstance(part, UserPromptPart)
    ]


async def _engine_after_outage() -> AgentEngine:
    """An engine whose last provider outcome was an outage: health is ``unavailable``."""
    engine = _engine([], failures=1)
    with pytest.raises(AgentRunError) as outage:
        await engine.run(_PROMPT, identity=_IDENTITY, conversation=None)
    assert outage.value.code is AgentRunErrorCode.PROVIDER_UNAVAILABLE
    assert (await engine.health()).status == "unavailable"
    return engine


async def _collect(engine: AgentEngine, conversation: Conversation | None) -> list[AgentEvent]:
    async with engine.run_stream(_PROMPT, identity=_IDENTITY, conversation=conversation) as events:
        return [event async for event in events]


def _final(events: list[AgentEvent]) -> FinalEvent:
    final = events[-1]
    assert isinstance(final, FinalEvent), events
    return final


def _assert_this_turn_only(raw: bytes | None) -> None:
    """The bytes hold this run's messages, each stamped with loom's id."""
    new = _decode(raw)

    assert tuple(message.kind for message in new) == _TURN_KINDS
    assert _prompts(new) == [_PROMPT]
    assert [message.conversation_id for message in new] == [_CONVERSATION_ID] * len(new)


class TestHistory:
    async def test_el_modelo_ve_el_historial_antes_del_prompt_cuando_run_lleva_conversacion(
        self,
    ) -> None:
        seen: list[list[ModelMessage]] = []
        engine = _engine(seen)

        await engine.run(_PROMPT, identity=_IDENTITY, conversation=_conversation(_history()))

        assert len(seen) == 1
        assert [message.kind for message in seen[0]] == ["request", "response", "request"]
        assert _prompts(seen[0]) == ["how is the cluster?", _PROMPT]

    async def test_devuelve_solo_los_mensajes_de_este_turno_cuando_run_lleva_conversacion(
        self,
    ) -> None:
        engine = _engine([])

        result = await engine.run(
            _PROMPT, identity=_IDENTITY, conversation=_conversation(_history())
        )

        _assert_this_turn_only(result.messages)

    async def test_el_final_lleva_los_mismos_mensajes_cuando_el_stream_lleva_conversacion(
        self,
    ) -> None:
        seen: list[list[ModelMessage]] = []
        engine = _engine(seen)

        events = await _collect(engine, _conversation(_history()))

        assert _prompts(seen[0]) == ["how is the cluster?", _PROMPT]
        _assert_this_turn_only(_final(events).messages)

    async def test_estampa_el_id_cuando_la_conversacion_no_tiene_historial(self) -> None:
        seen: list[list[ModelMessage]] = []
        engine = _engine(seen)

        result = await engine.run(_PROMPT, identity=_IDENTITY, conversation=_conversation())

        assert _prompts(seen[0]) == [_PROMPT]
        _assert_this_turn_only(result.messages)


class TestSingleShot:
    async def test_no_serializa_mensajes_cuando_run_no_lleva_conversacion(self) -> None:
        seen: list[list[ModelMessage]] = []
        engine = _engine(seen)

        result = await engine.run(_PROMPT, identity=_IDENTITY, conversation=None)

        assert _prompts(seen[0]) == [_PROMPT]
        assert result.messages is None

    async def test_no_serializa_mensajes_cuando_el_stream_no_lleva_conversacion(self) -> None:
        seen: list[list[ModelMessage]] = []
        engine = _engine(seen)

        events = await _collect(engine, None)

        assert len(seen) == 1
        assert _final(events).messages is None


class TestMalformedHistory:
    async def test_run_falla_sin_llamar_al_modelo_cuando_el_historial_no_es_json(self) -> None:
        seen: list[list[ModelMessage]] = []
        engine = _engine(seen)

        conversation = _conversation(b"not json")

        with pytest.raises(AgentRunError) as failure:
            await engine.run(_PROMPT, identity=_IDENTITY, conversation=conversation)

        assert failure.value.code is AgentRunErrorCode.CONVERSATION_LOAD_FAILED
        assert str(failure.value) == CONVERSATION_LOAD_FAILED_MESSAGE
        assert failure.value.usage is None
        assert seen == []

    async def test_el_stream_emite_un_error_sin_llamar_al_modelo_cuando_el_historial_no_es_json(
        self,
    ) -> None:
        seen: list[list[ModelMessage]] = []
        engine = _engine(seen)

        events = await _collect(engine, _conversation(b"not json"))

        assert events == [
            ErrorEvent(
                code=AgentRunErrorCode.CONVERSATION_LOAD_FAILED,
                message=CONVERSATION_LOAD_FAILED_MESSAGE,
            )
        ]
        assert seen == []

    async def test_health_conserva_la_caida_del_proveedor_cuando_run_rechaza_el_historial(
        self,
    ) -> None:
        """A history that does not decode is not a provider outcome.

        After an outage the engine reports ``unavailable``; a rejected history
        must not overwrite that last provider outcome, so health stays put.
        """
        engine = await _engine_after_outage()
        conversation = _conversation(b"not json")

        with pytest.raises(AgentRunError) as failure:
            await engine.run(_PROMPT, identity=_IDENTITY, conversation=conversation)

        assert failure.value.code is AgentRunErrorCode.CONVERSATION_LOAD_FAILED
        assert (await engine.health()).status == "unavailable"

    async def test_health_conserva_la_caida_del_proveedor_cuando_el_stream_rechaza_el_historial(
        self,
    ) -> None:
        """The streamed rejection leaves the last provider outcome untouched too."""
        engine = await _engine_after_outage()

        events = await _collect(engine, _conversation(b"not json"))

        assert isinstance(events[-1], ErrorEvent)
        assert events[-1].code is AgentRunErrorCode.CONVERSATION_LOAD_FAILED
        assert (await engine.health()).status == "unavailable"

    async def test_run_falla_cuando_el_historial_no_es_una_lista_de_mensajes(self) -> None:
        engine = _engine([])
        conversation = _conversation(b'{"kind": "request"}')

        with pytest.raises(AgentRunError) as failure:
            await engine.run(_PROMPT, identity=_IDENTITY, conversation=conversation)

        assert failure.value.code is AgentRunErrorCode.CONVERSATION_LOAD_FAILED


class TestRetries:
    async def test_reenvia_el_mismo_historial_cuando_run_reintenta(self) -> None:
        seen: list[list[ModelMessage]] = []
        engine = _engine(seen, retries=2, failures=2)

        result = await engine.run(
            _PROMPT, identity=_IDENTITY, conversation=_conversation(_history())
        )

        _assert_same_history_every_attempt(seen)
        _assert_this_turn_only(result.messages)

    async def test_reenvia_el_mismo_historial_cuando_el_stream_reintenta(self) -> None:
        seen: list[list[ModelMessage]] = []
        engine = _engine(seen, retries=2, failures=2)

        events = await _collect(engine, _conversation(_history()))

        _assert_same_history_every_attempt(seen)
        _assert_this_turn_only(_final(events).messages)


def _assert_same_history_every_attempt(seen: list[list[ModelMessage]]) -> None:
    assert len(seen) == 3
    prior = ModelMessagesTypeAdapter.validate_json(_history())
    for attempt in seen:
        assert len(attempt) == len(prior) + 1
        assert _prompts(attempt) == ["how is the cluster?", _PROMPT]
        assert [message.kind for message in attempt[:-1]] == [m.kind for m in prior]
