"""``ToolCallAccumulator``: the run's tool summary, built on the event path (011 T403).

Covers AC17: the records carry the tool name, the argument values and loom's
own outcome, in call order, and a call the run never answered is
distinguishable from a call that failed.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence

import msgspec

from loom.ai.abc import (
    AgentEvent,
    AgentUsage,
    FinalEvent,
    TextDeltaEvent,
    ToolCallEvent,
    ToolCallOutcome,
    ToolResultEvent,
)
from loom.ai.runtime._tool_calls import ToolCallAccumulator

_USAGE = AgentUsage(input_tokens=1, output_tokens=1, requests=1, duration_ms=1)


async def _stream(events: Sequence[AgentEvent]) -> AsyncIterator[AgentEvent]:
    """Yield a scripted stream of one run."""
    for event in events:
        yield event


async def _drain(
    accumulator: ToolCallAccumulator, events: Sequence[AgentEvent]
) -> list[AgentEvent]:
    """Run the whole script through the accumulator and return what came out."""
    return [event async for event in accumulator.track(_stream(events))]


async def test_registra_nombre_y_argumentos_de_cada_llamada() -> None:
    """The record carries the tool name and the argument values the model sent."""
    accumulator = ToolCallAccumulator()

    await _drain(
        accumulator,
        [ToolCallEvent(tool="get_incident", call_id="c1", arguments={"ref": "INC-1", "depth": 2})],
    )

    (record,) = accumulator.records()
    assert record.tool == "get_incident"
    assert record.call_id == "c1"
    assert record.arguments == {"ref": "INC-1", "depth": 2}


async def test_el_resultado_es_el_tipo_propio_del_command_y_no_el_evento_del_stream() -> None:
    """The outcome is a ``ToolCallOutcome``: a command carries no stream vocabulary.

    Encoding the record must not produce the stream's ``"type": "tool_result"``
    tag, and the outcome must not carry the stream's correlation id — the
    record already pairs the call with its own ``call_id``.
    """
    accumulator = ToolCallAccumulator()

    await _drain(
        accumulator,
        [
            ToolCallEvent(tool="get_incident", call_id="c1", arguments={}),
            ToolResultEvent(call_id="c1", ok=True, summary="3 rows"),
        ],
    )

    (record,) = accumulator.records()
    assert isinstance(record.result, ToolCallOutcome)
    encoded = msgspec.json.encode(record)
    assert b"tool_result" not in encoded
    assert msgspec.json.decode(encoded)["result"] == {"ok": True, "summary": "3 rows"}


async def test_preserva_el_orden_de_llamada_aunque_los_resultados_lleguen_cruzados() -> None:
    """Records are in call order, whatever order the results correlate in."""
    accumulator = ToolCallAccumulator()

    await _drain(
        accumulator,
        [
            ToolCallEvent(tool="first", call_id="c1", arguments={}),
            ToolCallEvent(tool="second", call_id="c2", arguments={}),
            ToolResultEvent(call_id="c2", ok=True, summary="7 rows"),
            ToolResultEvent(call_id="c1", ok=True, summary="ok"),
        ],
    )

    records = accumulator.records()
    assert [record.tool for record in records] == ["first", "second"]
    assert [record.result and record.result.summary for record in records] == ["ok", "7 rows"]


async def test_distingue_una_llamada_sin_resultado_de_una_fallida() -> None:
    """No result is ``None``; a failed call carries loom's outcome with ``ok`` false."""
    accumulator = ToolCallAccumulator()

    await _drain(
        accumulator,
        [
            ToolCallEvent(tool="unanswered", call_id="c1", arguments={}),
            ToolCallEvent(tool="refused", call_id="c2", arguments={}),
            ToolResultEvent(call_id="c2", ok=False, summary="refused"),
        ],
    )

    unanswered, refused = accumulator.records()
    assert unanswered.result is None
    assert refused.result is not None
    assert (refused.result.ok, refused.result.summary) == (False, "refused")


async def test_ignora_un_resultado_sin_llamada_observada() -> None:
    """A result whose ``call_id`` names no observed call invents no record."""
    accumulator = ToolCallAccumulator()

    await _drain(accumulator, [ToolResultEvent(call_id="ghost", ok=True, summary="ok")])

    assert accumulator.records() == ()


async def test_deja_pasar_todos_los_eventos_intactos() -> None:
    """The accumulator observes and forwards; it rewrites, drops and reorders nothing."""
    script: list[AgentEvent] = [
        TextDeltaEvent(text="thinking"),
        ToolCallEvent(tool="get_incident", call_id="c1", arguments={"ref": "INC-1"}),
        ToolResultEvent(call_id="c1", ok=True, summary="ok"),
        FinalEvent(output={"answer": "42"}, usage=_USAGE),
    ]
    accumulator = ToolCallAccumulator()

    forwarded = await _drain(accumulator, script)

    assert forwarded == script
    assert all(out is original for out, original in zip(forwarded, script, strict=True))


async def test_la_instantanea_no_comparte_estado_con_el_acumulador() -> None:
    """Records leave as an immutable snapshot, so a later call cannot mutate an old one."""
    accumulator = ToolCallAccumulator()
    stream = accumulator.track(
        _stream(
            [
                ToolCallEvent(tool="first", call_id="c1", arguments={}),
                ToolCallEvent(tool="second", call_id="c2", arguments={}),
            ]
        )
    )

    await anext(stream)
    early = accumulator.records()
    async for _ in stream:
        pass

    assert isinstance(early, tuple)
    assert [record.tool for record in early] == ["first"]
    assert [record.tool for record in accumulator.records()] == ["first", "second"]
