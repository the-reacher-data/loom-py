"""``max_usd`` is elastic: ``policies.on_unpriced_spend`` decides what a run
does when its cost cannot be fully computed (spec 016+correction).

The start-up probe (``_limits.py:warn_if_model_not_priceable``) only leaves a
notice; it never refuses to boot (``tests/unit/ai/engines/test_pydantic_ai_limits.py``).
What actually governs an unpriced response is this per-run guard
(``_engine.py:PydanticAIEngine._apply_unpriced_spend_policy``):
``on_unpriced_spend: serve`` (the default) answers anyway, since the provider
has already billed for the response regardless of whether loom could price
it, and records the gap; ``on_unpriced_spend: refuse`` fails the run instead,
coded ``COST_NOT_MEASURABLE`` — never ``USAGE_LIMIT_EXCEEDED``, because the
cap was never evaluated, not exceeded.
"""

from __future__ import annotations

import warnings
from collections.abc import AsyncIterator
from decimal import Decimal
from types import MappingProxyType
from typing import Any

import pytest
from pydantic_ai.exceptions import CostNotFoundWarning
from pydantic_ai.messages import (
    ModelMessage,
    ModelMessagesTypeAdapter,
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolCallPart,
    UserPromptPart,
)
from pydantic_ai.models import Model
from pydantic_ai.models.function import AgentInfo, DeltaToolCall, DeltaToolCalls, FunctionModel
from pydantic_ai.usage import RequestUsage

from loom.ai.abc import AgentEngine, Conversation, ErrorEvent, FinalEvent, HealthStatus
from loom.ai.declarative import PolicySpec
from loom.ai.engines.pydantic_ai import _engine as _engine_module
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.core.identity import Identity
from tests.helpers.pydantic_ai_engine import ScriptedUsageModel, build_engine, encode, make_plan

_IDENTITY = Identity(subject="bench-runner")
_PROMPT = "answer briefly"
_ANSWER = {"answer": "42"}
_CONVERSATION_ID = "c-cost-enforcement"


def _engine_with_unpriced_response(**policy_overrides: object) -> AgentEngine:
    """Bind a model whose one response never priced, in either policy."""
    plan = make_plan(policies=PolicySpec(retries=0, **policy_overrides))  # type: ignore[arg-type]
    usage = RequestUsage(input_tokens=8, output_tokens=8)  # cost intentionally left unset
    return build_engine(plan, ScriptedUsageModel(encode(_ANSWER), usage))


def _engine_pricing_only_the_second_call(**policy_overrides: object) -> AgentEngine:
    """Bind a model whose first response is unpriced and whose second one prices cleanly.

    Used to prove that :attr:`PydanticAIEngine._unpriced_spend_observed` is
    not cleared by a later clean run the way ``_last_failure`` is (correction
    point 4): both runs share one engine, so the same instance answers both.
    """
    plan = make_plan(policies=PolicySpec(retries=0, **policy_overrides))  # type: ignore[arg-type]
    return build_engine(plan, _model_pricing_only_the_second_call(encode(_ANSWER)))


def _model_pricing_only_the_second_call(payload: bytes) -> Model:
    text = payload.decode()
    calls = 0

    def respond(messages: list[Any], info: AgentInfo) -> ModelResponse:
        nonlocal calls
        calls += 1
        tool = info.output_tools[0].name
        usage = (
            RequestUsage(input_tokens=8, output_tokens=8)
            if calls == 1
            else RequestUsage(input_tokens=8, output_tokens=8, cost=Decimal("0.01"))
        )
        return ModelResponse(parts=[ToolCallPart(tool_name=tool, args=text)], usage=usage)

    return FunctionModel(respond)


def _model_counting_calls(payload: bytes, calls: list[int]) -> Model:
    """A model whose one response never prices, appending to *calls* on every call.

    Used by the regression below: a streamed structured-output agent's deltas
    are all output-tool deltas (``translate`` maps none of them to a loom
    event), so *this* model's streaming path is what exercises the
    ``not emitted`` branch a retry-storm used to slip through.
    """
    text = payload.decode()

    def respond(messages: list[Any], info: AgentInfo) -> ModelResponse:
        calls.append(1)
        tool = info.output_tools[0].name
        return ModelResponse(
            parts=[ToolCallPart(tool_name=tool, args=text)],
            usage=RequestUsage(input_tokens=8, output_tokens=8),
        )

    async def stream(messages: list[Any], info: AgentInfo) -> AsyncIterator[DeltaToolCalls]:
        calls.append(1)
        tool = info.output_tools[0].name
        yield {0: DeltaToolCall(name=tool, json_args=text, tool_call_id="scripted-call")}

    return FunctionModel(respond, stream_function=stream)


class TestServeIsTheDefault:
    """``on_unpriced_spend: serve`` answers, recording the gap rather than discarding it."""

    async def test_the_run_answers_even_though_its_cost_is_unpriced(self) -> None:
        engine = _engine_with_unpriced_response(max_usd=Decimal("2.00"))

        result = await engine.run(_PROMPT, identity=_IDENTITY)

        assert result.output == _ANSWER

    async def test_the_returned_usage_records_the_unpriced_response(self) -> None:
        engine = _engine_with_unpriced_response(max_usd=Decimal("2.00"))

        result = await engine.run(_PROMPT, identity=_IDENTITY)

        assert result.usage.cost is None
        assert result.usage.details["unpriced_requests"] == 1

    async def test_a_streamed_run_answers_and_records_the_same_gap(self) -> None:
        engine = _engine_with_unpriced_response(max_usd=Decimal("2.00"))

        async with engine.run_stream(_PROMPT, identity=_IDENTITY) as stream:
            events = [event async for event in stream]

        final = events[-1]
        assert isinstance(final, FinalEvent)
        assert final.usage.details["unpriced_requests"] == 1

    async def test_health_turns_degraded_once_an_unpriced_response_was_served(self) -> None:
        engine = _engine_with_unpriced_response(max_usd=Decimal("2.00"))

        await engine.run(_PROMPT, identity=_IDENTITY)

        health = await engine.health()
        assert health.status == "degraded"

    async def test_health_stays_degraded_after_a_later_run_prices_cleanly(self) -> None:
        """The gap is a property of the model, not of one run: a clean run does not clear it."""
        engine = _engine_pricing_only_the_second_call(max_usd=Decimal("2.00"))

        await engine.run(_PROMPT, identity=_IDENTITY)  # unpriced: sets the flag
        second = await engine.run(_PROMPT, identity=_IDENTITY)  # prices cleanly

        assert second.usage.cost == Decimal("0.01")
        assert (await engine.health()).status == "degraded"

    async def test_a_run_with_no_declared_cap_is_unaffected_by_an_unpriced_response(self) -> None:
        """No ``max_usd`` means nothing to fail closed on: the run answers with no notice."""
        engine = _engine_with_unpriced_response()

        result = await engine.run(_PROMPT, identity=_IDENTITY)

        assert result.usage.cost is None
        assert "unpriced_requests" not in result.usage.details
        assert (await engine.health()).status == "ok"

    async def test_a_run_whose_response_carries_a_cost_is_unaffected(self) -> None:
        """A response that does price stays within budget, with nothing to record."""
        plan = make_plan(policies=PolicySpec(retries=0, max_usd=Decimal("2.00")))
        usage = RequestUsage(input_tokens=8, output_tokens=8, cost=Decimal("0.01"))
        engine = build_engine(plan, ScriptedUsageModel(encode(_ANSWER), usage))

        result = await engine.run(_PROMPT, identity=_IDENTITY)

        assert result.usage.cost == Decimal("0.01")
        assert "unpriced_requests" not in result.usage.details


class TestRefuseFailsTheRun:
    """``on_unpriced_spend: refuse`` fails instead of serving an unpriced answer."""

    async def test_run_fails_with_cost_not_measurable_not_usage_limit_exceeded(self) -> None:
        engine = _engine_with_unpriced_response(max_usd=Decimal("2.00"), on_unpriced_spend="refuse")

        with pytest.raises(AgentRunError) as failure:
            await engine.run(_PROMPT, identity=_IDENTITY)

        assert failure.value.code == AgentRunErrorCode.COST_NOT_MEASURABLE
        assert failure.value.code != AgentRunErrorCode.USAGE_LIMIT_EXCEEDED

    async def test_the_failure_carries_what_the_run_had_already_spent(self) -> None:
        engine = _engine_with_unpriced_response(max_usd=Decimal("2.00"), on_unpriced_spend="refuse")

        with pytest.raises(AgentRunError) as failure:
            await engine.run(_PROMPT, identity=_IDENTITY)

        assert failure.value.usage is not None
        assert failure.value.usage.input_tokens == 8

    async def test_the_failure_is_not_retried_despite_its_infrastructure_class(self) -> None:
        """``COST_NOT_MEASURABLE`` is ``INFRASTRUCTURE``-classed, but
        :func:`~loom.ai.errors.is_retriable` carves it out explicitly:
        retrying an already-billed, deterministic gap cannot land on a
        different outcome. For :meth:`~loom.ai.abc.AgentEngine.run`, the
        policy also surfaces only after :meth:`_run_with_retries` has already
        returned, so a declared ``retries`` never even reaches the check
        here — see the streaming counterpart below for the shape where that
        alone was not enough."""
        plan = make_plan(
            policies=PolicySpec(retries=2, max_usd=Decimal("2.00"), on_unpriced_spend="refuse")
        )
        usage = RequestUsage(input_tokens=8, output_tokens=8)
        engine = build_engine(plan, ScriptedUsageModel(encode(_ANSWER), usage))

        with pytest.raises(AgentRunError) as failure:
            await engine.run(_PROMPT, identity=_IDENTITY)

        assert failure.value.usage is not None
        assert failure.value.usage.requests == 1

    async def test_a_streamed_run_fails_the_same_way(self) -> None:
        engine = _engine_with_unpriced_response(max_usd=Decimal("2.00"), on_unpriced_spend="refuse")

        async with engine.run_stream(_PROMPT, identity=_IDENTITY) as stream:
            events = [event async for event in stream]

        terminal = events[-1]
        assert isinstance(terminal, ErrorEvent)
        assert terminal.code == AgentRunErrorCode.COST_NOT_MEASURABLE

    async def test_a_streamed_structured_output_run_calls_the_model_once_despite_retries(
        self,
    ) -> None:
        """A structured-output agent's provider deltas are all output-tool deltas,
        which ``translate`` does not map to a loom event, so no delta is ever
        emitted before the terminal frame. That used to make ``_may_retry`` see
        ``not emitted`` and retry a deterministic, already-billed refusal —
        multiplying the very bill ``on_unpriced_spend: refuse`` exists to stop.
        The model must answer exactly once, however many retries are declared."""
        calls: list[int] = []
        plan = make_plan(
            policies=PolicySpec(retries=2, max_usd=Decimal("2.00"), on_unpriced_spend="refuse")
        )
        engine = build_engine(plan, _model_counting_calls(encode(_ANSWER), calls))

        async with engine.run_stream(_PROMPT, identity=_IDENTITY) as stream:
            events = [event async for event in stream]

        terminal = events[-1]
        assert isinstance(terminal, ErrorEvent)
        assert terminal.code == AgentRunErrorCode.COST_NOT_MEASURABLE
        assert len(calls) == 1

    async def test_a_run_whose_response_carries_a_cost_is_unaffected_by_refuse(self) -> None:
        """A fully priced response never triggers ``refuse``: there is nothing to act on."""
        plan = make_plan(
            policies=PolicySpec(retries=0, max_usd=Decimal("2.00"), on_unpriced_spend="refuse")
        )
        usage = RequestUsage(input_tokens=8, output_tokens=8, cost=Decimal("0.01"))
        engine = build_engine(plan, ScriptedUsageModel(encode(_ANSWER), usage))

        result = await engine.run(_PROMPT, identity=_IDENTITY)

        assert result.usage.cost == Decimal("0.01")


class TestOnUnpricedSpendIsInertWithoutACap:
    """``on_unpriced_spend`` governs an existing cap; it does nothing on its own."""

    async def test_refuse_without_max_usd_does_not_fail_an_unpriced_response(self) -> None:
        engine = _engine_with_unpriced_response(on_unpriced_spend="refuse")

        result = await engine.run(_PROMPT, identity=_IDENTITY)

        assert result.output == _ANSWER
        assert result.usage.cost is None
        assert (await engine.health()).status == "ok"


def _unpriced_prior_turn() -> bytes:
    """A history whose one prior response never priced (default ``RequestUsage``).

    ``model_name`` is set deliberately (unlike O9's synthetic-response case): this
    fixture proves the exclusion below on its own terms — a real provider response,
    from a prior run, that must be excluded because it is not *this* run's own, not
    because it looks synthetic.
    """
    prior: list[ModelMessage] = [
        ModelRequest(parts=[UserPromptPart(content="first turn")]),
        ModelResponse(
            parts=[TextPart(content="a first answer, never priced")], model_name="priced"
        ),
    ]
    return ModelMessagesTypeAdapter.dump_json(prior)


def _second_turn(**policy_overrides: object) -> tuple[AgentEngine, Conversation]:
    """An engine whose own response prices cleanly, continuing a conversation whose
    only prior response never priced."""
    plan = make_plan(policies=PolicySpec(retries=0, **policy_overrides))  # type: ignore[arg-type]
    usage = RequestUsage(input_tokens=8, output_tokens=8, cost=Decimal("0.01"))
    engine = build_engine(plan, ScriptedUsageModel(encode(_ANSWER), usage, model_name="priced"))
    conversation = Conversation(conversation_id=_CONVERSATION_ID, history=_unpriced_prior_turn())
    return engine, conversation


class TestExcludesThePriorTurnsHistory:
    """The count ``on_unpriced_spend`` acts on is this run's own responses:
    ``result.new_messages()``, never ``result.all_messages()``, which also walks
    the injected ``message_history``."""

    async def test_a_run_with_unpriced_history_and_a_priced_answer_is_not_refused(self) -> None:
        engine, conversation = _second_turn(max_usd=Decimal("2.00"), on_unpriced_spend="refuse")

        result = await engine.run(_PROMPT, identity=_IDENTITY, conversation=conversation)

        assert result.output == _ANSWER
        assert result.usage.cost == Decimal("0.01")
        assert "unpriced_requests" not in result.usage.details
        assert (await engine.health()).status == "ok"

    async def test_a_streamed_run_with_unpriced_history_and_a_priced_answer_is_not_refused(
        self,
    ) -> None:
        engine, conversation = _second_turn(max_usd=Decimal("2.00"), on_unpriced_spend="refuse")

        async with engine.run_stream(_PROMPT, identity=_IDENTITY, conversation=conversation) as (
            stream
        ):
            events = [event async for event in stream]

        final = events[-1]
        assert isinstance(final, FinalEvent)
        assert final.usage.cost == Decimal("0.01")
        assert "unpriced_requests" not in final.usage.details
        assert (await engine.health()).status == "ok"


class TestHealthReflectsARefusedRun:
    """``health()`` must not read ``ok`` after every run has been refused:
    the gap ``on_unpriced_spend: refuse`` fails over is the same gap ``serve``
    would have recorded, and it must be observable the same way."""

    async def test_health_is_degraded_after_a_run_refused_for_unpriced_spend(self) -> None:
        engine = _engine_with_unpriced_response(max_usd=Decimal("2.00"), on_unpriced_spend="refuse")

        with pytest.raises(AgentRunError) as failure:
            await engine.run(_PROMPT, identity=_IDENTITY)
        assert failure.value.code == AgentRunErrorCode.COST_NOT_MEASURABLE

        health = await engine.health()
        assert health.status == "degraded"

    async def test_health_is_degraded_after_a_streamed_run_refused_for_unpriced_spend(
        self,
    ) -> None:
        engine = _engine_with_unpriced_response(max_usd=Decimal("2.00"), on_unpriced_spend="refuse")

        async with engine.run_stream(_PROMPT, identity=_IDENTITY) as stream:
            events = [event async for event in stream]
        terminal = events[-1]
        assert isinstance(terminal, ErrorEvent)
        assert terminal.code == AgentRunErrorCode.COST_NOT_MEASURABLE

        health = await engine.health()
        assert health.status == "degraded"


class TestLastObservedFailureIsRecordedTheSameWayForRunAndStream:
    """``PydanticAIEngine._record`` must be the single writer of the internal
    ``_last_failure`` code, reached by every ``AgentRunError`` that leaves the
    engine, whichever of :meth:`~loom.ai.abc.AgentEngine.run` or
    :meth:`~loom.ai.abc.AgentEngine.run_stream` raised it. ``COST_NOT_MEASURABLE``
    is absent from ``_HEALTH_BY_CODE`` today, so both paths already read
    ``degraded`` by way of the separate ``_unpriced_spend_observed`` flag —
    this test proves the two paths would still agree the day someone maps that
    code, by mapping it here, rather than leaving that agreement to chance."""

    async def test_run_and_stream_report_the_same_mapped_health_once_the_code_is_mapped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mapped = HealthStatus(status="unavailable", detail="the refused run's cost is unmeasurable")
        by_code = {**_engine_module._HEALTH_BY_CODE, AgentRunErrorCode.COST_NOT_MEASURABLE: mapped}
        monkeypatch.setattr(_engine_module, "_HEALTH_BY_CODE", MappingProxyType(by_code))

        run_engine = _engine_with_unpriced_response(
            max_usd=Decimal("2.00"), on_unpriced_spend="refuse"
        )
        with pytest.raises(AgentRunError):
            await run_engine.run(_PROMPT, identity=_IDENTITY)

        stream_engine = _engine_with_unpriced_response(
            max_usd=Decimal("2.00"), on_unpriced_spend="refuse"
        )
        async with stream_engine.run_stream(_PROMPT, identity=_IDENTITY) as stream:
            [_ async for _ in stream]

        assert (await run_engine.health()) == mapped
        assert (await stream_engine.health()) == mapped


class TestSurvivesCostNotFoundWarningElevatedToAnError:
    """``pydantic_ai._warnings.CostNotFoundWarning`` subclasses ``Warning``, not
    ``UserWarning``: a deployment that runs under ``-W error`` (or pytest's
    ``filterwarnings = error``) turns it into an exception ``agent.run``
    raises. ``classify`` must still land on ``COST_NOT_MEASURABLE``, not the
    unclassified-outage default, or the failure becomes retriable and the
    model is billed again for an already-deterministic gap."""

    async def test_the_run_fails_cost_not_measurable_and_calls_the_model_once(self) -> None:
        calls: list[int] = []
        plan = make_plan(
            policies=PolicySpec(retries=2, max_usd=Decimal("2.00"), on_unpriced_spend="refuse")
        )
        engine = build_engine(plan, _model_counting_calls(encode(_ANSWER), calls))

        with warnings.catch_warnings():
            warnings.simplefilter("error", CostNotFoundWarning)
            with pytest.raises(AgentRunError) as failure:
                await engine.run(_PROMPT, identity=_IDENTITY)

        assert failure.value.code == AgentRunErrorCode.COST_NOT_MEASURABLE
        assert len(calls) == 1

    async def test_health_is_degraded_afterwards_even_though_the_policy_never_ran(self) -> None:
        """The warning is raised from inside the provider call, before
        :meth:`~loom.ai.engines.pydantic_ai._engine.PydanticAIEngine._apply_unpriced_spend_policy`
        gets a chance to run, so the flag it normally sets cannot be the only
        way ``health()`` learns about the gap (spec 016+correction, follow-up)."""
        engine = _engine_with_unpriced_response(max_usd=Decimal("2.00"), on_unpriced_spend="refuse")

        with warnings.catch_warnings():
            warnings.simplefilter("error", CostNotFoundWarning)
            with pytest.raises(AgentRunError) as failure:
                await engine.run(_PROMPT, identity=_IDENTITY)
        assert failure.value.code == AgentRunErrorCode.COST_NOT_MEASURABLE

        health = await engine.health()
        assert health.status == "degraded"

    async def test_health_is_degraded_afterwards_for_a_streamed_run_too(self) -> None:
        """Same guarantee, the streamed counterpart of the test above: the
        warning is raised from inside ``run_stream_events`` (``_one_run``),
        before :meth:`~loom.ai.engines.pydantic_ai._engine.PydanticAIEngine._conclude`
        ever calls the policy."""
        calls: list[int] = []
        plan = make_plan(
            policies=PolicySpec(retries=2, max_usd=Decimal("2.00"), on_unpriced_spend="refuse")
        )
        engine = build_engine(plan, _model_counting_calls(encode(_ANSWER), calls))

        with warnings.catch_warnings():
            warnings.simplefilter("error", CostNotFoundWarning)
            async with engine.run_stream(_PROMPT, identity=_IDENTITY) as stream:
                events = [event async for event in stream]
        terminal = events[-1]
        assert isinstance(terminal, ErrorEvent)
        assert terminal.code == AgentRunErrorCode.COST_NOT_MEASURABLE

        health = await engine.health()
        assert health.status == "degraded"
