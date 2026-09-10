"""``hook_command``: the nested, filtered command the output hook feeds its use case (002 T5)."""

from __future__ import annotations

from typing import Any

import msgspec
import pytest

from loom.ai.compiler._plan import (
    HOOK_CONTEXT_FIELDS,
    HOOK_MESSAGES_FIELD,
    HOOK_OUTPUT_FIELD,
    AgentPlan,
    CompiledInstruction,
    CompiledOutput,
)
from loom.ai.declarative import PolicySpec
from loom.ai.inference import InferenceTarget
from loom.ai.runtime._bounded import RunContext
from loom.ai.runtime._hooks import hook_command
from loom.core.command import Command
from loom.core.identity import Identity

_ALL_NAMES = frozenset({HOOK_OUTPUT_FIELD, HOOK_MESSAGES_FIELD, *HOOK_CONTEXT_FIELDS})


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
        instructions=(CompiledInstruction(text="answer"),),
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


def test_nests_the_output_when_it_is_a_dict(run: RunContext) -> None:
    """A dict output is nested verbatim under ``output``."""
    command = hook_command({"answer": "42"}, run, _ALL_NAMES)

    assert command["output"] == {"answer": "42"}


def test_converts_the_output_to_builtins_when_it_is_a_struct(run: RunContext) -> None:
    """A struct output is offered as builtins so any Input type can convert it back."""
    command = hook_command(_Report(severity="high", confidence=0.7), run, _ALL_NAMES)

    assert command["output"] == {"severity": "high", "confidence": 0.7}


def test_offers_the_runs_context_when_the_input_accepts_it(run: RunContext) -> None:
    """Every context name carries the run's, the identity's or the plan's value."""
    command = hook_command({}, run, _ALL_NAMES)

    assert command == {
        "output": {},
        "messages": None,
        "interaction_id": "int-1",
        "conversation_id": "c-42",
        "subject": "user-1",
        "mechanism": "test",
        "agent": "incident-triage",
        "provider": "fake",
        "model": "fake-model",
    }


def test_filters_to_the_accepted_names_when_the_input_declares_fewer(run: RunContext) -> None:
    """Names the Input does not declare never reach it."""
    command = hook_command({"answer": "42"}, run, frozenset({"output", "agent"}))

    assert command == {"output": {"answer": "42"}, "agent": "incident-triage"}


def test_does_not_let_the_output_shadow_the_context_when_names_collide(
    run: RunContext,
) -> None:
    """An output field named ``subject`` stays nested; the context wins."""
    command = hook_command({"subject": "spoofed"}, run, _ALL_NAMES)

    assert command["subject"] == "user-1"
    assert command["output"] == {"subject": "spoofed"}


def test_decodes_a_strict_command_when_filtered_to_its_names(run: RunContext) -> None:
    """A ``forbid_unknown_fields`` Command accepts the filtered dict."""
    accepted = frozenset(info.name for info in msgspec.structs.fields(_StrictCommand))

    instance, seen = _StrictCommand.from_payload(hook_command({"answer": "42"}, run, accepted))

    assert instance == _StrictCommand(output={"answer": "42"}, interaction_id="int-1")
    assert seen == frozenset({"output", "interaction_id"})


def test_offers_exactly_the_names_the_compiler_promises(run: RunContext) -> None:
    """The run-time command and the compile-time offer are one contract, not two lists."""
    command = hook_command({}, run, _ALL_NAMES)

    assert set(command) == {HOOK_OUTPUT_FIELD, HOOK_MESSAGES_FIELD, *HOOK_CONTEXT_FIELDS}


def test_offers_the_messages_when_the_run_carries_them(run: RunContext) -> None:
    """The run's serialised new messages are offered verbatim under ``messages``."""
    command = hook_command({}, run, _ALL_NAMES, messages=b"[]")

    assert command[HOOK_MESSAGES_FIELD] == b"[]"


def test_filters_out_the_messages_when_the_input_does_not_declare_them(run: RunContext) -> None:
    """A Command not declaring ``messages`` never receives them."""
    command = hook_command({}, run, frozenset({"output"}), messages=b"[]")

    assert command == {"output": {}}


def test_offers_messages_none_when_not_given(run: RunContext) -> None:
    """A single-shot run offers ``None``, so an optional field keeps its default."""
    command = hook_command({}, run, _ALL_NAMES)

    assert command[HOOK_MESSAGES_FIELD] is None
