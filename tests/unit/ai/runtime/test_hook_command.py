"""``hook_command``: the nested, filtered command the output hook feeds its use case (002 T5)."""

from __future__ import annotations

from typing import Any

import msgspec
import pytest

from loom.ai.abc import ToolCallOutcome, ToolCallRecord
from loom.ai.compiler._plan import (
    HOOK_CONTEXT_FIELDS,
    HOOK_MESSAGES_FIELD,
    HOOK_OUTPUT_FIELD,
    HOOK_TOOL_CALLS_FIELD,
    AgentPlan,
    CompiledOutput,
)
from loom.ai.declarative import PolicySpec
from loom.ai.inference import InferenceTarget
from loom.ai.runtime._bounded import RunContext
from loom.ai.runtime._hooks import hook_command
from loom.core.command import Command
from loom.core.identity import Identity

_ALL_NAMES = frozenset(
    {HOOK_OUTPUT_FIELD, HOOK_MESSAGES_FIELD, HOOK_TOOL_CALLS_FIELD, *HOOK_CONTEXT_FIELDS}
)

_RECORD = ToolCallRecord(
    tool="get_incident",
    call_id="c1",
    arguments={"ref": "INC-1"},
    result=ToolCallOutcome(ok=True, summary="3 rows"),
)


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
def run() -> RunContext:
    """One admitted run of a verified caller."""
    return RunContext(
        plan=_plan(),
        identity=Identity(subject="user-1", roles=("analyst",), mechanism="test"),
        interaction_id="int-1",
        conversation_id="c-42",
    )


def test_anida_el_output_cuando_es_un_dict(run: RunContext) -> None:
    """A dict output is nested verbatim under ``output``."""
    command = hook_command({"answer": "42"}, run, _ALL_NAMES)

    assert command["output"] == {"answer": "42"}


def test_convierte_el_output_a_builtins_cuando_es_un_struct(run: RunContext) -> None:
    """A struct output is offered as builtins so any Input type can convert it back."""
    command = hook_command(_Report(severity="high", confidence=0.7), run, _ALL_NAMES)

    assert command["output"] == {"severity": "high", "confidence": 0.7}


def test_ofrece_el_contexto_del_run_cuando_el_input_lo_acepta(run: RunContext) -> None:
    """Every context name carries the run's, the identity's or the plan's value."""
    command = hook_command({}, run, _ALL_NAMES)

    assert command == {
        "output": {},
        "messages": None,
        "tool_calls": (),
        "interaction_id": "int-1",
        "conversation_id": "c-42",
        "subject": "user-1",
        "mechanism": "test",
        "agent": "incident-triage",
        "provider": "fake",
        "model": "fake-model",
    }


def test_filtra_a_los_nombres_aceptados_cuando_el_input_declara_menos(run: RunContext) -> None:
    """Names the Input does not declare never reach it."""
    command = hook_command({"answer": "42"}, run, frozenset({"output", "agent"}))

    assert command == {"output": {"answer": "42"}, "agent": "incident-triage"}


def test_no_deja_que_el_output_suplante_al_contexto_cuando_comparte_nombres(
    run: RunContext,
) -> None:
    """An output field named ``subject`` stays nested; the context wins."""
    command = hook_command({"subject": "spoofed"}, run, _ALL_NAMES)

    assert command["subject"] == "user-1"
    assert command["output"] == {"subject": "spoofed"}


def test_decodifica_un_command_estricto_cuando_se_filtra_a_sus_nombres(run: RunContext) -> None:
    """A ``forbid_unknown_fields`` Command accepts the filtered dict."""
    accepted = frozenset(info.name for info in msgspec.structs.fields(_StrictCommand))

    instance, seen = _StrictCommand.from_payload(hook_command({"answer": "42"}, run, accepted))

    assert instance == _StrictCommand(output={"answer": "42"}, interaction_id="int-1")
    assert seen == frozenset({"output", "interaction_id"})


def test_ofrece_exactamente_los_nombres_que_el_compilador_promete(run: RunContext) -> None:
    """The run-time command and the compile-time offer are one contract, not two lists."""
    command = hook_command({}, run, _ALL_NAMES)

    assert set(command) == {
        HOOK_OUTPUT_FIELD,
        HOOK_MESSAGES_FIELD,
        HOOK_TOOL_CALLS_FIELD,
        *HOOK_CONTEXT_FIELDS,
    }


def test_ofrece_los_messages_cuando_el_run_los_lleva(run: RunContext) -> None:
    """The run's serialised new messages are offered verbatim under ``messages``."""
    command = hook_command({}, run, _ALL_NAMES, messages=b"[]")

    assert command[HOOK_MESSAGES_FIELD] == b"[]"


def test_filtra_los_messages_cuando_el_input_no_los_declara(run: RunContext) -> None:
    """A Command not declaring ``messages`` never receives them."""
    command = hook_command({}, run, frozenset({"output"}), messages=b"[]")

    assert command == {"output": {}}


def test_ofrece_messages_none_cuando_no_se_indican(run: RunContext) -> None:
    """A single-shot run offers ``None``, so an optional field keeps its default."""
    command = hook_command({}, run, _ALL_NAMES)

    assert command[HOOK_MESSAGES_FIELD] is None


def test_ofrece_los_tool_calls_cuando_el_run_los_acumulo(run: RunContext) -> None:
    """The accumulated records are offered verbatim, in call order (011/L19)."""
    command = hook_command({}, run, _ALL_NAMES, tool_calls=[_RECORD])

    assert command[HOOK_TOOL_CALLS_FIELD] == (_RECORD,)


def test_filtra_los_tool_calls_cuando_el_input_no_los_declara(run: RunContext) -> None:
    """A Command not declaring ``tool_calls`` never receives them."""
    command = hook_command({}, run, frozenset({"output"}), tool_calls=[_RECORD])

    assert command == {"output": {}}


def test_ofrece_tool_calls_vacio_cuando_no_se_acumulo_nada(run: RunContext) -> None:
    """A hook that declared the name but saw no tool traffic gets an empty tuple."""
    command = hook_command({}, run, _ALL_NAMES)

    assert command[HOOK_TOOL_CALLS_FIELD] == ()
