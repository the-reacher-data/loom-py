"""Every counter the engine reports reaches the caller (A1).

The rule under test is propagation, not curation: loom does not decide which
counters matter, so a counter :class:`~loom.ai.abc.AgentUsage` does not name
still arrives — under the engine's own name, in ``details`` — and a model the
engine could not price reports no cost rather than a zero that would win a
cost comparison it never entered.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from msgspec import structs
from pydantic_ai.usage import RequestUsage

from loom.ai.abc import AgentEngine, AgentUsage, ErrorEvent, FinalEvent
from loom.ai.errors import AgentRunError
from loom.core.identity import Identity
from tests.helpers.pydantic_ai_engine import (
    STRICT_SCHEMA,
    ScriptedUsageModel,
    build_engine,
    encode,
    make_plan,
)

_IDENTITY = Identity(subject="bench-runner")
_PROMPT = "compare the models"
_ANSWER = {"answer": "42"}

_FUTURE_COUNTER = "counter_added_by_a_newer_release"
"""A counter no release of loom knows about, set the way a provider sets one."""

# ``tool_calls`` is a run-level counter the engine increments itself, so no
# per-request script can set it: it is asserted against a run that really calls
# a tool, in ``tests/integration/ai/test_capabilities.py``.


def _reported_usage(*, cost: Decimal | None = Decimal("0.0417")) -> RequestUsage:
    """Return the accounting the scripted model reports for its one request."""
    usage = RequestUsage(
        input_tokens=1840,
        output_tokens=412,
        cache_read_tokens=1200,
        cache_write_tokens=64,
        input_audio_tokens=96,
        output_audio_tokens=32,
        cache_audio_read_tokens=16,
        details={"reasoning_tokens": 128},
        cost=cost,
    )
    setattr(usage, _FUTURE_COUNTER, 7)
    return usage


@pytest.fixture
def engine() -> AgentEngine:
    """The real adapter over a model reporting every counter it can."""
    return build_engine(
        make_plan(schema=STRICT_SCHEMA), ScriptedUsageModel(encode(_ANSWER), _reported_usage())
    )


@pytest.fixture
def failing_engine() -> AgentEngine:
    """The real adapter over a model that spends and then answers off-schema."""
    return build_engine(
        make_plan(schema=STRICT_SCHEMA),
        ScriptedUsageModel(encode({"unexpected": "field"}), _reported_usage()),
    )


@pytest.fixture
def unpriced_engine() -> AgentEngine:
    """The real adapter over a model the engine could not price."""
    return build_engine(
        make_plan(schema=STRICT_SCHEMA),
        ScriptedUsageModel(encode(_ANSWER), _reported_usage(cost=None)),
    )


async def _final_usage(engine: AgentEngine) -> AgentUsage:
    """Run *engine* as a stream and return the usage of its terminal event."""
    async with engine.run_stream(_PROMPT, identity=_IDENTITY) as stream:
        events = [event async for event in stream]
    terminal = events[-1]
    assert isinstance(terminal, FinalEvent), f"the stream must end in a final event: {terminal!r}"
    return terminal.usage


class TestReportedCounters:
    """A run surfaces the whole accounting the engine reported for it."""

    async def test_names_the_counters_any_engine_reports(self, engine: AgentEngine) -> None:
        """The named fields carry what the model reported, counter by counter."""
        usage = (await engine.run(_PROMPT, identity=_IDENTITY)).usage

        assert usage.input_tokens == 1840
        assert usage.output_tokens == 412
        assert usage.requests == 1
        assert usage.cost == Decimal("0.0417")

    async def test_keeps_the_cache_split_the_model_reported(self, engine: AgentEngine) -> None:
        """Cached and fresh input tokens stay apart: a cached token costs a fraction."""
        usage = (await engine.run(_PROMPT, identity=_IDENTITY)).usage

        assert usage.cache_read_tokens == 1200
        assert usage.cache_write_tokens == 64
        assert usage.cache_hit_ratio == pytest.approx(1200 / 1840)

    async def test_carries_a_counter_it_has_no_field_for_in_details(
        self, engine: AgentEngine
    ) -> None:
        """A counter no loom release knows about still reaches the caller."""
        usage = (await engine.run(_PROMPT, identity=_IDENTITY)).usage

        assert usage.details[_FUTURE_COUNTER] == 7

    async def test_carries_the_modality_counters_in_details(self, engine: AgentEngine) -> None:
        """The audio counters and the engine's own details ride under their own names."""
        usage = (await engine.run(_PROMPT, identity=_IDENTITY)).usage

        assert usage.details["input_audio_tokens"] == 96
        assert usage.details["output_audio_tokens"] == 32
        assert usage.details["cache_audio_read_tokens"] == 16
        assert usage.details["reasoning_tokens"] == 128


class TestUnpricedModel:
    """A model with no price entry reports an absent cost, never a zero one."""

    async def test_reports_no_cost_when_the_model_has_no_price(
        self, unpriced_engine: AgentEngine
    ) -> None:
        """An unknown cost stays unknown: a zero would silently win a comparison."""
        usage = (await unpriced_engine.run(_PROMPT, identity=_IDENTITY)).usage

        assert usage.cost is None

    async def test_keeps_the_token_counters_when_the_cost_is_absent(
        self, unpriced_engine: AgentEngine
    ) -> None:
        """An unpriced run still accounts for everything else it spent."""
        usage = (await unpriced_engine.run(_PROMPT, identity=_IDENTITY)).usage

        assert usage.input_tokens == 1840
        assert usage.cache_read_tokens == 1200


class TestStreamingParity:
    """The terminal event of a stream accounts for the run exactly as a run does."""

    async def test_final_event_matches_the_non_streaming_result(self, engine: AgentEngine) -> None:
        """Same run, same accounting: only the wall clock is allowed to differ."""
        completed = (await engine.run(_PROMPT, identity=_IDENTITY)).usage
        streamed = await _final_usage(engine)

        assert structs.replace(streamed, duration_ms=0) == structs.replace(completed, duration_ms=0)

    async def test_final_event_carries_the_cost(self, engine: AgentEngine) -> None:
        """A streamed run is compared on cost like any other."""
        assert (await _final_usage(engine)).cost == Decimal("0.0417")


class TestFailedRun:
    """A run that spends and then fails still reports what it burned.

    Otherwise a model that fails often ranks better on cost than one that
    answers, which inverts the comparison this accounting exists to serve.
    """

    async def test_the_error_carries_what_the_run_spent(self, failing_engine: AgentEngine) -> None:
        """The coded failure carries the partial accounting of the failed run."""
        with pytest.raises(AgentRunError) as failure:
            await failing_engine.run(_PROMPT, identity=_IDENTITY)

        usage = failure.value.usage
        assert usage is not None
        assert usage.input_tokens >= 1840
        assert usage.requests >= 1
        assert usage.cost is not None
        assert usage.cost >= Decimal("0.0417")

    async def test_the_terminal_error_event_carries_it_too(
        self, failing_engine: AgentEngine
    ) -> None:
        """A stream reports the spend of a failed run on its terminal event."""
        async with failing_engine.run_stream(_PROMPT, identity=_IDENTITY) as stream:
            events = [event async for event in stream]

        terminal = events[-1]
        assert isinstance(terminal, ErrorEvent), f"expected a terminal error: {terminal!r}"
        assert terminal.usage is not None
        assert terminal.usage.requests >= 1
        assert terminal.usage.cost is not None
