"""Wire the shared engine contract suite to the deterministic fake (T045).

Maps each neutral :class:`ContractScenario` onto a scripted
:class:`FakeAgentEngine`; the suite itself asserts nothing fake-specific, so
the same call will later run against the real engine adapter unmodified.
"""

from __future__ import annotations

from loom.ai.abc import AgentEngine, ErrorEvent
from loom.testing import ContractScenario, FakeAgentEngine, agent_engine_contract_suite


def _engine_for(scenario: ContractScenario) -> AgentEngine:
    """Build a fake engine exhibiting the scenario's behaviour."""
    if scenario.error_code is not None:
        script = (ErrorEvent(code=scenario.error_code, message="scripted failure"),)
        return FakeAgentEngine(script=script)
    if scenario.events is not None:
        return FakeAgentEngine(script=scenario.events)
    return FakeAgentEngine(output=scenario.expected_output)


def test_fake_engine_satisfies_the_shared_agent_engine_contract() -> None:
    agent_engine_contract_suite(_engine_for)
