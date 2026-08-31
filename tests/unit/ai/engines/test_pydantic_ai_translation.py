"""Tool-call and tool-result translation: the two contracts of design D7/D8.

``tool_result.summary`` is produced by the platform from a closed list of
shapes (FR-030b): a tool can put whatever it likes in its payload and none of
it may ever become the summary. ``tool_call.arguments`` is a decoded mapping,
decoded exactly once and never able to turn a model mistake into a crash or a
leak of the raw argument string.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest
from pydantic_ai.messages import (
    FunctionToolCallEvent,
    FunctionToolResultEvent,
    ToolCallPart,
    ToolReturnPart,
)

from loom.ai.abc import ToolCallEvent, ToolResultEvent
from loom.ai.engines.pydantic_ai._events import translate

CANARY = "SECRET-CANARY-9931"
"""Token planted in the tool payload; it must never reach the summary."""

PAYLOAD = f'[{{"secret": "{CANARY}"}}]'
"""A tool payload a capability legitimately returns to the model."""


def _result_event(
    *, content: object, metadata: Mapping[str, Any] | None = None
) -> FunctionToolResultEvent:
    """Build the engine event carrying one completed tool return."""
    part = ToolReturnPart(
        tool_name="sql_reporting",
        content=content,
        tool_call_id="call-1",
        metadata=dict(metadata) if metadata is not None else None,
    )
    return FunctionToolResultEvent(part=part)


def _call_event(args: object) -> FunctionToolCallEvent:
    """Build the engine event carrying one tool call with ``args``."""
    return FunctionToolCallEvent(
        part=ToolCallPart(tool_name="sql_reporting", args=args, tool_call_id="call-1")
    )


def _translated_result(event: FunctionToolResultEvent) -> ToolResultEvent:
    translated = translate(event)
    assert isinstance(translated, ToolResultEvent)
    return translated


def _translated_call(event: FunctionToolCallEvent) -> ToolCallEvent:
    translated = translate(event)
    assert isinstance(translated, ToolCallEvent)
    return translated


class TestToolResultSummary:
    @pytest.mark.parametrize(
        ("shape", "expected"),
        [
            ({"shape": "rows", "n": 142}, "142 rows"),
            ({"shape": "ok"}, "ok"),
            ({"shape": "bytes", "n": 3184}, "ok"),
            ({"shape": "something-new"}, "ok"),
        ],
    )
    def test_el_resumen_es_una_forma_cerrada_cuando_el_resultado_trae_metadata(
        self, shape: Mapping[str, Any], expected: str
    ) -> None:
        """The summary is built from structured facts, from a closed list."""
        event = _result_event(content=PAYLOAD, metadata={"loom": dict(shape)})

        assert _translated_result(event).summary == expected

    def test_el_resumen_no_lleva_ningun_byte_del_payload_cuando_el_resultado_es_grande(
        self,
    ) -> None:
        """No substring of the payload may appear in the summary (FR-030b)."""
        event = _result_event(content=PAYLOAD, metadata={"loom": {"shape": "rows", "n": 142}})

        assert CANARY not in _translated_result(event).summary

    def test_el_resumen_ignora_el_texto_libre_cuando_la_herramienta_intenta_dictarlo(
        self,
    ) -> None:
        """A free-form string supplied by a tool can never become the summary."""
        event = _result_event(
            content=PAYLOAD,
            metadata={"loom": {"shape": "rows", "n": 142, "summary": CANARY}},
        )

        assert _translated_result(event).summary == "142 rows"

    def test_el_resumen_cae_en_ok_cuando_el_resultado_no_trae_metadata(self) -> None:
        """An absent shape degrades to ``ok``, never to the payload."""
        event = _result_event(content=PAYLOAD)

        assert _translated_result(event).summary == "ok"


class TestRefusedResults:
    def test_el_resultado_no_es_ok_cuando_la_herramienta_rechaza_por_valor(self) -> None:
        """A refusal must not read as a normal call in the stream (FR-046b)."""
        event = _result_event(
            content="refused: the result has 5 rows, above the max_rows bound of 2",
            metadata={"loom": {"shape": "refused"}},
        )

        translated = _translated_result(event)

        assert (translated.ok, translated.summary) == (False, "refused")

    def test_el_resumen_del_rechazo_no_lleva_el_motivo_cuando_la_herramienta_lo_publica(
        self,
    ) -> None:
        """The reason stays in the payload: no tool text may reach the summary."""
        event = _result_event(
            content=f"refused: {CANARY}",
            metadata={"loom": {"shape": "refused", "n": 5, "summary": CANARY}},
        )

        assert CANARY not in _translated_result(event).summary


class TestToolCallArguments:
    def test_los_argumentos_se_decodifican_a_un_mapping_vacio_cuando_el_json_es_invalido(
        self,
    ) -> None:
        """A malformed argument string yields ``{}``: no crash, no raw leak."""
        valid = _translated_call(_call_event('{"sql": "SELECT 1"}')).arguments
        malformed = _translated_call(_call_event("{not json")).arguments

        assert (valid, malformed) == ({"sql": "SELECT 1"}, {})

    def test_los_argumentos_se_conservan_cuando_el_motor_ya_entrega_un_mapping(self) -> None:
        """Already-decoded arguments are carried through, never re-encoded."""
        assert _translated_call(_call_event({"sql": "SELECT 1"})).arguments == {"sql": "SELECT 1"}
