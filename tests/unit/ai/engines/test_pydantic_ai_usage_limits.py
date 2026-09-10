"""Spend caps that bind the run itself (spec 016, T502, AC-008).

``usage_limits`` (T501) is a pure projection; this suite asserts that the
projected ``UsageLimits`` actually reaches the engine's own ``agent.run`` and
``agent.run_stream_events`` calls and stops a run that would exceed it, in
both run modes, carrying the usage the run had already spent.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic_ai.usage import RequestUsage

from loom.ai.abc import AgentEngine, ErrorEvent
from loom.ai.declarative import PolicySpec
from loom.ai.errors import AgentRunError, AgentRunErrorCode, is_retriable
from loom.core.identity import Identity
from tests.helpers.pydantic_ai_engine import ScriptedUsageModel, build_engine, encode, make_plan

_IDENTITY = Identity(subject="bench-runner")
_PROMPT = "answer briefly"
_ANSWER = {"answer": "42"}


def _over_budget_engine(**policy_overrides: object) -> AgentEngine:
    plan = make_plan(policies=PolicySpec(retries=0, **policy_overrides))  # type: ignore[arg-type]
    usage = RequestUsage(input_tokens=8, output_tokens=8, cost=Decimal("0.01"))
    return build_engine(plan, ScriptedUsageModel(encode(_ANSWER), usage))


class TestUsageLimitStopsARun:
    """AC-008: ``policies.max_total_tokens`` bounds a run that would exceed it."""

    async def test_run_fails_with_the_coded_usage_limit_error(self) -> None:
        engine = _over_budget_engine(max_total_tokens=10)

        with pytest.raises(AgentRunError) as failure:
            await engine.run(_PROMPT, identity=_IDENTITY)

        assert failure.value.code == AgentRunErrorCode.USAGE_LIMIT_EXCEEDED

    async def test_the_error_carries_the_usage_already_spent(self) -> None:
        engine = _over_budget_engine(max_total_tokens=10)

        with pytest.raises(AgentRunError) as failure:
            await engine.run(_PROMPT, identity=_IDENTITY)

        usage = failure.value.usage
        assert usage is not None
        assert usage.input_tokens == 8
        assert usage.output_tokens == 8

    async def test_the_failure_class_is_not_one_loom_retries(self) -> None:
        engine = _over_budget_engine(max_total_tokens=10)

        with pytest.raises(AgentRunError) as failure:
            await engine.run(_PROMPT, identity=_IDENTITY)

        assert not is_retriable(failure.value.code)

    async def test_a_streamed_run_fails_the_same_way(self) -> None:
        engine = _over_budget_engine(max_total_tokens=10)

        async with engine.run_stream(_PROMPT, identity=_IDENTITY) as stream:
            events = [event async for event in stream]

        terminal = events[-1]
        assert isinstance(terminal, ErrorEvent)
        assert terminal.code == AgentRunErrorCode.USAGE_LIMIT_EXCEEDED
        assert terminal.usage is not None

    async def test_a_run_within_budget_is_unaffected(self) -> None:
        """A generous cap changes nothing: the projection is not a hidden narrowing."""
        engine = _over_budget_engine(max_total_tokens=1000)

        result = await engine.run(_PROMPT, identity=_IDENTITY)

        assert result.usage.input_tokens == 8
