"""Instructions phase: the string form compiles, the block form fails closed.

Compiling an authored :class:`~loom.ai.declarative.InstructionBlock` sequence
into ``AgentPlan.instructions`` is T202; this phase only guards the artifact
against reaching a plan while that compilation does not exist (spec 016).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import loom.ai.compiler  # noqa: F401  — red until the compiler exists
from loom.ai.declarative import AgentSpecV1, InstructionBlock
from loom.ai.errors import AgentCompilationIssue, AgentErrorCode
from tests.unit.ai.phases.conftest import SOURCE_PATH


def test_a_string_artifact_compiles_exactly_as_before(
    spec_factory: Callable[..., AgentSpecV1],
    plan_for: Callable[..., Any],
) -> None:
    spec = spec_factory(instructions="Answer using only the prompt. Say so when unsure.")

    plan = plan_for(spec)

    assert plan.instructions == "Answer using only the prompt. Say so when unsure."


def test_a_block_sequence_artifact_fails_closed_naming_the_artifact_and_field(
    spec_factory: Callable[..., AgentSpecV1],
    single_issue_for: Callable[..., AgentCompilationIssue],
) -> None:
    spec = spec_factory(instructions=(InstructionBlock(text="You are the triage assistant."),))

    issue = single_issue_for(spec)

    assert issue.code == AgentErrorCode.INSTRUCTION_BLOCK_INVALID
    assert issue.field == "instructions"
    assert issue.component == SOURCE_PATH
