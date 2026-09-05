"""A ``skills`` grant without the harness extra fails naming the extra to install."""

from __future__ import annotations

import sys

import pytest

from loom.ai.compiler._plan import CompiledSkillsCapability
from loom.ai.engines.pydantic_ai._capabilities import build_capabilities
from loom.ai.errors import AgentCompilationError, AgentErrorCode


def test_falla_nombrando_ai_harness_cuando_el_harness_no_esta_instalado(
    monkeypatch: pytest.MonkeyPatch, tmp_path: object
) -> None:
    """The message names ``ai-harness``, not the missing module."""
    monkeypatch.setitem(sys.modules, "pydantic_ai_harness", None)
    plan = _plan_with_skills()

    with pytest.raises(AgentCompilationError) as failure:
        build_capabilities(plan)

    issue = failure.value.issues[0]
    assert issue.code is AgentErrorCode.PROVIDER_NOT_INSTALLED
    assert "ai-harness" in issue.message


def _plan_with_skills() -> object:
    """Minimal stand-in carrying one compiled ``skills`` grant."""

    class _Plan:
        capabilities = (
            CompiledSkillsCapability(
                library="./skills", directory="/tmp/skills", names=("triage",)
            ),
        )

    return _Plan()
