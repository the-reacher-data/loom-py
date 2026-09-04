"""``hook_command``: the nested, filtered command the output hook feeds its use case (002 T5)."""

from __future__ import annotations

from typing import Any

import msgspec
import pytest

from loom.ai.compiler._plan import HOOK_CONTEXT_FIELDS, HOOK_OUTPUT_FIELD, AgentPlan, CompiledOutput
from loom.ai.declarative import PolicySpec
from loom.ai.inference import InferenceTarget
from loom.ai.runtime._hooks import HookRun, hook_command
from loom.core.command import Command
from loom.core.identity import Identity

_ALL_NAMES = frozenset({HOOK_OUTPUT_FIELD, *HOOK_CONTEXT_FIELDS})


class _Report(msgspec.Struct, frozen=True, kw_only=True):
    severity: str
    confidence: float


class _StrictCommand(Command, frozen=True, kw_only=True, forbid_unknown_fields=True):
    output: dict[str, Any]
    interaction_id: str


def _plan() -> AgentPlan:
    return AgentPlan(
        name="incident-triage",
        description="test agent",
        instructions="answer",
        spec_version=1,
        inference=InferenceTarget(provider="fake", model="fake-model"),
        output=CompiledOutput(schema={"type": "object"}, decoder=msgspec.json.Decoder(dict)),
        capabilities=(),
        policies=PolicySpec(),
        metadata={},
    )


@pytest.fixture
def run() -> HookRun:
    """One admitted run of a verified caller."""
    return HookRun(
        plan=_plan(),
        identity=Identity(subject="user-1", roles=("analyst",), mechanism="test"),
        interaction_id="int-1",
        conversation_id="c-42",
    )


def test_anida_el_output_cuando_es_un_dict(run: HookRun) -> None:
    """A dict output is nested verbatim under ``output``."""
    command = hook_command({"answer": "42"}, run, _ALL_NAMES)

    assert command["output"] == {"answer": "42"}


def test_convierte_el_output_a_builtins_cuando_es_un_struct(run: HookRun) -> None:
    """A struct output is offered as builtins so any Input type can convert it back."""
    command = hook_command(_Report(severity="high", confidence=0.7), run, _ALL_NAMES)

    assert command["output"] == {"severity": "high", "confidence": 0.7}


def test_ofrece_el_contexto_del_run_cuando_el_input_lo_acepta(run: HookRun) -> None:
    """Every context name carries the run's, the identity's or the plan's value."""
    command = hook_command({}, run, _ALL_NAMES)

    assert command == {
        "output": {},
        "interaction_id": "int-1",
        "conversation_id": "c-42",
        "subject": "user-1",
        "mechanism": "test",
        "agent": "incident-triage",
        "provider": "fake",
        "model": "fake-model",
    }


def test_filtra_a_los_nombres_aceptados_cuando_el_input_declara_menos(run: HookRun) -> None:
    """Names the Input does not declare never reach it."""
    command = hook_command({"answer": "42"}, run, frozenset({"output", "agent"}))

    assert command == {"output": {"answer": "42"}, "agent": "incident-triage"}


def test_no_deja_que_el_output_suplante_al_contexto_cuando_comparte_nombres(
    run: HookRun,
) -> None:
    """An output field named ``subject`` stays nested; the context wins."""
    command = hook_command({"subject": "spoofed"}, run, _ALL_NAMES)

    assert command["subject"] == "user-1"
    assert command["output"] == {"subject": "spoofed"}


def test_decodifica_un_command_estricto_cuando_se_filtra_a_sus_nombres(run: HookRun) -> None:
    """A ``forbid_unknown_fields`` Command accepts the filtered dict."""
    accepted = frozenset(info.name for info in msgspec.structs.fields(_StrictCommand))

    instance, seen = _StrictCommand.from_payload(hook_command({"answer": "42"}, run, accepted))

    assert instance == _StrictCommand(output={"answer": "42"}, interaction_id="int-1")
    assert seen == frozenset({"output", "interaction_id"})


def test_ofrece_exactamente_los_nombres_que_el_compilador_promete(run: HookRun) -> None:
    """The run-time command and the compile-time offer are one contract, not two lists."""
    command = hook_command({}, run, _ALL_NAMES)

    assert set(command) == {HOOK_OUTPUT_FIELD, *HOOK_CONTEXT_FIELDS}
