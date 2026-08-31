"""Deterministic fake engine contract for ``loom.testing.agents`` (T039/T040).

Pins the two guarantees of the testing surface's ``FakeAgentEngine``:

* **T039 — byte-for-byte reproducibility**: two fresh instances built from the
  same script (or from no script at all) must produce ``msgspec``-identical
  bytes for both ``run()`` and the full ``run_stream()`` event sequence.
* **T040 — no network, no credentials**: the fake must never open a socket
  and must work with every provider API key stripped from the environment.

These tests are written before the implementation exists; until
``src/loom/testing/agents.py`` lands they fail at collection with
``ImportError`` — that red state is expected.
"""

from __future__ import annotations

import socket
from collections.abc import Sequence
from typing import Any, NoReturn

import msgspec
import pytest

from loom.ai.abc import (
    AgentEvent,
    AgentResult,
    AgentUsage,
    FinalEvent,
    HealthStatus,
    TextDeltaEvent,
    ToolCallEvent,
    ToolResultEvent,
)
from loom.core.identity import Identity
from loom.testing import FakeAgentEngine

_IDENTITY = Identity(subject="tester")
_PROMPT = "resume el estado del pipeline"

_USAGE = AgentUsage(input_tokens=12, output_tokens=34, requests=2, duration_ms=56)

_SCRIPT: tuple[AgentEvent, ...] = (
    TextDeltaEvent(text="Consultando "),
    ToolCallEvent(tool="lookup", call_id="call-1", arguments={"query": "pipeline"}),
    ToolResultEvent(call_id="call-1", ok=True, summary="1 fila"),
    TextDeltaEvent(text="listo."),
    FinalEvent(output={"answer": "todo verde"}, usage=_USAGE),
)

_OUTPUT: dict[str, Any] = {"answer": "todo verde"}


def _build_scripted_engine() -> FakeAgentEngine:
    """Build one engine from the shared fixed script."""
    return FakeAgentEngine(script=_SCRIPT, output=_OUTPUT)


async def _execute(engine: FakeAgentEngine) -> bytes:
    """Run the engine both ways and serialize everything observable to bytes."""
    result: AgentResult = await engine.run(_PROMPT, identity=_IDENTITY)
    events: list[AgentEvent] = []
    async with engine.run_stream(_PROMPT, identity=_IDENTITY) as stream:
        async for event in stream:
            events.append(event)
    return msgspec.json.encode({"result": result, "events": events})


class TestReproducibilidadT039:
    """T039 — same script, same bytes; no clocks, no randomness."""

    async def test_run_y_stream_producen_bytes_identicos_cuando_el_guion_es_el_mismo(
        self,
    ) -> None:
        first = await _execute(_build_scripted_engine())
        second = await _execute(_build_scripted_engine())

        assert first == second

    async def test_el_stream_reproduce_exactamente_el_guion_cuando_se_pasa_script(
        self,
    ) -> None:
        engine = _build_scripted_engine()

        events: list[AgentEvent] = []
        async with engine.run_stream(_PROMPT, identity=_IDENTITY) as stream:
            async for event in stream:
                events.append(event)

        assert tuple(events) == _SCRIPT

    async def test_dos_instancias_frescas_producen_bytes_identicos_cuando_no_hay_guion(
        self,
    ) -> None:
        first = await _execute(FakeAgentEngine(output=_OUTPUT))
        second = await _execute(FakeAgentEngine(output=_OUTPUT))

        assert first == second

    async def test_el_stream_por_defecto_termina_en_final_con_el_output_cuando_no_hay_guion(
        self,
    ) -> None:
        engine = FakeAgentEngine(output=_OUTPUT)

        events: list[AgentEvent] = []
        async with engine.run_stream(_PROMPT, identity=_IDENTITY) as stream:
            async for event in stream:
                events.append(event)

        assert isinstance(events[-1], FinalEvent) and events[-1].output == _OUTPUT

    async def test_run_devuelve_el_output_del_guion_cuando_el_guion_termina_en_final(
        self,
    ) -> None:
        engine = _build_scripted_engine()

        result = await engine.run(_PROMPT, identity=_IDENTITY)

        assert result == AgentResult(output=_OUTPUT, usage=_USAGE)


class TestSinRedNiCredencialesT040:
    """T040 — the fake never opens sockets and needs no provider keys."""

    @pytest.fixture(autouse=True)
    def block_network(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Fail hard on any socket creation and strip provider credentials."""

        def _no_socket(*args: object, **kwargs: object) -> NoReturn:
            raise AssertionError("network access attempted")

        monkeypatch.setattr(socket.socket, "__init__", _no_socket)
        monkeypatch.setattr(socket, "create_connection", _no_socket)
        for key in ("OPENAI_API_KEY", "ANTHROPIC_API_KEY"):
            monkeypatch.delenv(key, raising=False)

    async def test_run_no_abre_sockets_cuando_la_red_esta_bloqueada(self) -> None:
        engine = _build_scripted_engine()

        result = await engine.run(_PROMPT, identity=_IDENTITY)

        assert result.usage == _USAGE

    async def test_run_stream_completo_no_abre_sockets_cuando_la_red_esta_bloqueada(
        self,
    ) -> None:
        engine = _build_scripted_engine()

        events: list[AgentEvent] = []
        async with engine.run_stream(_PROMPT, identity=_IDENTITY) as stream:
            async for event in stream:
                events.append(event)

        assert len(events) == len(_SCRIPT)

    async def test_health_no_abre_sockets_cuando_la_red_esta_bloqueada(self) -> None:
        engine = _build_scripted_engine()

        status: HealthStatus = await engine.health()

        assert status.status == "ok"

    async def test_el_guion_por_defecto_funciona_sin_credenciales_de_proveedor(
        self,
    ) -> None:
        engine = FakeAgentEngine(output=_OUTPUT)

        result = await engine.run(_PROMPT, identity=_IDENTITY)

        assert result.output == _OUTPUT


def _typecheck_script_param(script: Sequence[AgentEvent]) -> FakeAgentEngine:
    """Pin that the constructor accepts any ``Sequence[AgentEvent]``."""
    return FakeAgentEngine(script=script, output=None)
