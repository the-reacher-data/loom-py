"""A ``skills`` grant without the harness extra fails naming the extra to install."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from loom.ai.compiler._plan import CompiledSkillsCapability
from loom.ai.declarative import PolicySpec
from loom.ai.engines.pydantic_ai._capabilities import build_capabilities
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.core.di import LoomContainer


def test_names_the_ai_harness_extra_when_the_harness_is_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The message names ``ai-harness``, not the missing module."""
    monkeypatch.setitem(sys.modules, "pydantic_ai_harness", None)
    plan = _plan_with_skills(tmp_path)

    with pytest.raises(AgentCompilationError) as failure:
        build_capabilities(plan, LoomContainer())

    issue = failure.value.issues[0]
    assert issue.code is AgentErrorCode.PROVIDER_NOT_INSTALLED
    assert "ai-harness" in issue.message


def _plan_with_skills(directory: Path) -> object:
    """Minimal stand-in carrying one compiled ``skills`` grant."""

    class _Plan:
        name = "analyst"
        policies = PolicySpec()
        capabilities = (
            CompiledSkillsCapability(
                library="./skills", directory=str(directory), names=("triage",)
            ),
        )

    return _Plan()
