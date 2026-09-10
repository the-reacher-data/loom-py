"""``_errors.py:classify`` totality, and behaviour for ``CostCalculationFailedWarning``.

``pydantic_ai._warnings`` declares three classes today: one subclasses
``UserWarning`` (visible by default, never elevated by ``-W error``'s bare
``Warning`` filter) and two subclass ``Warning`` directly. Both of the latter
are load-bearing for ``classify`` under ``-W error`` (see "Spend caps" in
``docs/ai/artifacts.md``): an unmapped one falls through to
``PROVIDER_UNAVAILABLE``, which is retriable, and a deterministic,
already-billed gap gets billed again on every retry.

The totality test below is the trinquete: it walks every subclass of
``Warning`` in that module that is not itself a ``UserWarning``, so a future
pydantic-ai release adding a third one fails this suite at the dependency
bump, instead of surfacing as a tripled bill in production.
"""

from __future__ import annotations

import warnings
from typing import Any

import pytest
from pydantic_ai import _cost as pydantic_ai_cost
from pydantic_ai import _warnings as pydantic_ai_warnings
from pydantic_ai.exceptions import CostCalculationFailedWarning
from pydantic_ai.messages import ModelMessage, ModelResponse, ToolCallPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.usage import RequestUsage

from loom.ai.declarative import PolicySpec
from loom.ai.engines.pydantic_ai._errors import _EXCEPTION_CODES, classify
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.core.identity import Identity
from tests.helpers.pydantic_ai_engine import build_engine, encode, make_plan

_IDENTITY = Identity(subject="bench-runner")
_PROMPT = "answer briefly"


def _bare_warning_classes() -> list[type[Warning]]:
    """Return every ``pydantic_ai._warnings`` class that is not a ``UserWarning``.

    ``-W error`` treats a bare ``Warning`` and a ``UserWarning`` differently
    only through the *filter* an operator declares — but pydantic-ai's own
    ``PydanticAIDeprecationWarning`` documents that it chose ``UserWarning``
    specifically to stay visible under the *default* filter, which already
    excludes it from this concern. Every sibling that instead subclasses
    ``Warning`` directly needs an explicit entry in ``_EXCEPTION_CODES``.
    """
    return [
        member
        for member in vars(pydantic_ai_warnings).values()
        if isinstance(member, type)
        and issubclass(member, Warning)
        and not issubclass(member, UserWarning)
    ]


class TestClassifyIsTotalOverPydanticAisBareWarnings:
    def test_every_bare_warning_subclass_is_mapped(self) -> None:
        unmapped = [cls for cls in _bare_warning_classes() if cls not in _EXCEPTION_CODES]

        assert unmapped == [], (
            f"{[cls.__name__ for cls in unmapped]} subclass Warning directly "
            "(not UserWarning), so -W error elevates them to exceptions "
            "classify() must recognise; add each to _EXCEPTION_CODES in "
            "loom/ai/engines/pydantic_ai/_errors.py"
        )

    def test_each_bare_warning_classifies_to_cost_not_measurable(self) -> None:
        """Both of today's entries land on the same terminal code: an unpriced,
        already-billed gap, never an unclassified, retriable outage."""
        for cls in _bare_warning_classes():
            assert classify(cls("boom")) == AgentRunErrorCode.COST_NOT_MEASURABLE


def _raise_unexpected_pricing_failure(*args: object, **kwargs: object) -> Any:
    raise TypeError("genai-prices blew up unexpectedly")


def _model_counting_calls(payload: bytes, calls: list[int]) -> FunctionModel:
    text = payload.decode()

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        calls.append(1)
        tool = info.output_tools[0].name
        return ModelResponse(
            parts=[ToolCallPart(tool_name=tool, args=text)],
            usage=RequestUsage(input_tokens=8, output_tokens=8),
            model_name="scripted",
        )

    return FunctionModel(respond)


class TestSurvivesCostCalculationFailedWarningElevatedToAnError:
    """``pydantic_ai._cost.best_effort_price`` degrades ``LookupError`` and
    ``ValueError`` silently, but anything else it catches is re-emitted as
    ``CostCalculationFailedWarning`` — a bare ``Warning``, exactly like its
    sibling ``CostNotFoundWarning``. Unlike that sibling, this one is not
    gated behind a declared ``cost_limit``: pricing runs on every response,
    so an artifact with no spend cap declared at all is exposed too."""

    async def test_the_run_fails_cost_not_measurable_and_calls_the_model_once(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            pydantic_ai_cost, "calculate_price_for_usage", _raise_unexpected_pricing_failure
        )
        calls: list[int] = []
        plan = make_plan(policies=PolicySpec(retries=2))
        engine = build_engine(plan, _model_counting_calls(encode({"answer": "42"}), calls))

        with warnings.catch_warnings():
            warnings.simplefilter("error", CostCalculationFailedWarning)
            with pytest.raises(AgentRunError) as failure:
                await engine.run(_PROMPT, identity=_IDENTITY)

        assert failure.value.code == AgentRunErrorCode.COST_NOT_MEASURABLE
        assert len(calls) == 1
