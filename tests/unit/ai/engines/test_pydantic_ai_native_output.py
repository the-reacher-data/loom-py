"""Native pydantic output through pydantic-ai's own ``output_type`` (D7, FR-012, spec US5).

Drives the real engine adapter — :class:`~loom.ai.engines.pydantic_ai.PydanticAIEngineProvider`
and :class:`~loom.ai.engines.pydantic_ai._engine.PydanticAIEngine` — against a
``FunctionModel``, no network, in the shape of ``test_pydantic_ai_output_check.py``.
Pins US5 items 1-6:

* a pydantic ``type_ref``'s validation failure (a ``model_validator`` rejecting
  a business rule) is fed back to the model by pydantic-ai itself and retried,
  bounded by ``policies.retries``, with no loom decode anywhere in the loop;
* exhaustion classifies as ``OUTPUT_SCHEMA_VIOLATION``, the same coded failure
  a ``msgspec.Struct`` answer gets, but a Struct twin fails at the first
  attempt (no bridge to pydantic-ai's own retries exists for it);
* ``output_check`` still runs for a pydantic output, receiving the validated
  instance projected to builtins, and cannot substitute the answer;
* a run-level ``expect=`` override still wins over the plan's own pydantic
  output;
* ``output_mode: tool | native | absent`` wraps the model class the same way
  it wraps a msgspec schema, and a native-mode stream withholds a rejected
  attempt's deltas exactly as ``output_check`` does.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from typing import Any

import msgspec
import pytest
from pydantic_ai import NativeOutput, ToolOutput
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models import Model
from pydantic_ai.models.function import AgentInfo, DeltaToolCall, DeltaToolCalls, FunctionModel

from loom.ai.abc import OutputCheck, TextDeltaEvent
from loom.ai.compiler import AgentPlan
from loom.ai.compiler.phases._output import compile_output
from loom.ai.declarative import TypeRefOutput
from loom.ai.engines.pydantic_ai._engine import PydanticAIEngine
from loom.ai.engines.pydantic_ai._spec import build_output_type
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.inference import InferenceTarget
from loom.core.identity import Identity
from tests.helpers.pydantic_ai_engine import STRICT_SCHEMA, build_engine, encode, make_plan

pytestmark = pytest.mark.usefixtures("fake_myapp_path")

_IDENTITY = Identity(subject="caller")
_REF = "myapp.domain.pydantic_invoices:StrictInvoiceModel"

_BAD = encode({"issuer": "acme", "total": -5.0})
_GOOD = encode({"issuer": "acme", "total": 5.0})
_PROSE = "a plain sentence with no declared shape at all"


def _pydantic_plan(
    *,
    mode: str | None = "tool",
    retries: int = 0,
    output_check: OutputCheck | None = None,
) -> AgentPlan:
    """A plan whose output is the strict pydantic ``StrictInvoiceModel`` type_ref."""
    output, issues = compile_output(TypeRefOutput(ref=_REF), "contract")
    assert output is not None, issues
    plan = make_plan(schema=STRICT_SCHEMA, retries=retries, output_check=output_check)
    plan = msgspec.structs.replace(plan, output=output)
    inference = msgspec.structs.replace(plan.inference, output_mode=mode)
    return msgspec.structs.replace(plan, inference=inference)


def _tool_model(payloads: list[bytes], seen: list[list[ModelMessage]]) -> Model:
    """A model answering one payload per call, by an output-tool call."""
    calls = {"n": -1}

    def _payload() -> bytes:
        calls["n"] += 1
        return payloads[min(calls["n"], len(payloads) - 1)]

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        seen.append(list(messages))
        tool = info.output_tools[0].name
        return ModelResponse(parts=[ToolCallPart(tool_name=tool, args=_payload().decode())])

    async def stream(
        messages: list[ModelMessage], info: AgentInfo
    ) -> AsyncIterator[DeltaToolCalls]:
        seen.append(list(messages))
        tool = info.output_tools[0].name
        yield {0: DeltaToolCall(name=tool, json_args=_payload().decode(), tool_call_id="call")}

    return FunctionModel(respond, stream_function=stream)


def _native_text_model(payloads: list[bytes]) -> Model:
    """A model answering one payload per call, as a text part (native output)."""
    calls = {"n": -1}

    def _payload() -> str:
        calls["n"] += 1
        return payloads[min(calls["n"], len(payloads) - 1)].decode()

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        del messages, info
        return ModelResponse(parts=[TextPart(content=_payload())])

    async def stream(messages: list[ModelMessage], info: AgentInfo) -> AsyncIterator[str]:
        del messages, info
        yield _payload()

    return FunctionModel(respond, stream_function=stream)


def _prose_model() -> Model:
    """A model that answers free text, never a structured tool call."""

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        del messages, info
        return ModelResponse(parts=[TextPart(content=_PROSE)])

    async def stream(messages: list[ModelMessage], info: AgentInfo) -> AsyncIterator[str]:
        del messages, info
        yield _PROSE

    return FunctionModel(respond, stream_function=stream)


class TestARejectedAttemptIsRetriedByPydanticAiItself:
    async def test_the_second_answer_is_the_model_instance_and_two_requests_were_made(self) -> None:
        seen_by_model: list[list[ModelMessage]] = []
        plan = _pydantic_plan(retries=1)
        engine = build_engine(plan, _tool_model([_BAD, _GOOD], seen_by_model))

        result = await engine.run("hi", identity=_IDENTITY)

        assert type(result.output).__name__ == "StrictInvoiceModel"
        assert result.output.issuer == "acme"
        assert result.output.total == 5.0
        assert result.usage.requests == 2
        assert len(seen_by_model) == 2

    async def test_the_stream_also_completes_with_the_model_instance(self) -> None:
        seen_by_model: list[list[ModelMessage]] = []
        plan = _pydantic_plan(retries=1)
        engine = build_engine(plan, _tool_model([_BAD, _GOOD], seen_by_model))

        async with engine.run_stream("hi", identity=_IDENTITY) as events:
            collected = [event async for event in events]

        final = collected[-1]
        assert type(final.output).__name__ == "StrictInvoiceModel"  # type: ignore[union-attr]
        assert final.output.total == 5.0  # type: ignore[union-attr]
        assert len(seen_by_model) == 2


class TestExhaustionClassifiesAsOutputSchemaViolation:
    async def test_every_attempt_rejected_fails_once_retries_are_spent(self) -> None:
        seen_by_model: list[list[ModelMessage]] = []
        plan = _pydantic_plan(retries=1)
        engine = build_engine(plan, _tool_model([_BAD], seen_by_model))

        with pytest.raises(AgentRunError) as excinfo:
            await engine.run("hi", identity=_IDENTITY)

        assert excinfo.value.code is AgentRunErrorCode.OUTPUT_SCHEMA_VIOLATION
        assert len(seen_by_model) == 2  # retries=1: the original attempt, plus one retry

    async def test_a_struct_twin_fails_at_the_first_attempt_with_no_bridge_to_retries(self) -> None:
        """A ``msgspec.Struct`` answer has no pydantic-ai output-type validation at all:
        loom's own decode runs once, after the (single) provider call, byte-for-byte
        as before this feature."""
        seen_by_model: list[list[ModelMessage]] = []
        plan = make_plan(schema=STRICT_SCHEMA, retries=1)
        bad = encode({"answer": 123})
        engine = build_engine(plan, _tool_model([bad], seen_by_model))

        with pytest.raises(AgentRunError) as excinfo:
            await engine.run("hi", identity=_IDENTITY)

        assert excinfo.value.code is AgentRunErrorCode.OUTPUT_SCHEMA_VIOLATION
        assert len(seen_by_model) == 1


class TestOutputCheckReceivesBuiltinsAndCannotSubstituteTheAnswer:
    async def test_the_check_receives_to_builtins_of_the_instance(self) -> None:
        seen_by_check: list[Mapping[str, Any]] = []

        def check(answer: Mapping[str, Any]) -> str | None:
            seen_by_check.append(dict(answer))
            return None

        plan = _pydantic_plan(output_check=check)
        engine = build_engine(plan, _tool_model([_GOOD], []))

        result = await engine.run("hi", identity=_IDENTITY)

        assert seen_by_check == [{"issuer": "acme", "total": 5.0}]
        assert type(result.output).__name__ == "StrictInvoiceModel"

    async def test_a_mutating_check_cannot_alter_the_returned_answer(self) -> None:
        def mutate_and_accept(answer: Mapping[str, Any]) -> str | None:
            mutated = dict(answer)
            mutated["issuer"] = "mutated"
            return None

        plan = _pydantic_plan(output_check=mutate_and_accept)
        engine = build_engine(plan, _tool_model([_GOOD], []))

        result = await engine.run("hi", identity=_IDENTITY)

        assert result.output.issuer == "acme"


class TestARunLevelShapeOverrideStillWins:
    async def test_expect_wins_over_the_plans_own_pydantic_output(self) -> None:
        plan = _pydantic_plan(retries=1)
        engine = build_engine(plan, _prose_model())
        assert isinstance(engine, PydanticAIEngine)

        async with engine.run_stream_shaped("hi", identity=_IDENTITY, output_type=str) as events:
            final = [event async for event in events][-1]

        assert final.output == _PROSE  # type: ignore[union-attr]


class TestNativeModeWithholdsARejectedAttempt:
    async def test_a_rejected_attempt_reaches_no_subscriber(self) -> None:
        inference = InferenceTarget(provider="openai", model="gpt-5.2", output_mode="native")
        plan = _pydantic_plan(mode="native", retries=1)
        plan = msgspec.structs.replace(plan, inference=inference)
        engine = build_engine(plan, _native_text_model([_BAD, _GOOD]))

        async with engine.run_stream("hi", identity=_IDENTITY) as events:
            collected = [event async for event in events]

        deltas = [event.text for event in collected if isinstance(event, TextDeltaEvent)]
        assert deltas == [_GOOD.decode()]
        assert all("-5" not in text for text in deltas)


class TestBuildOutputTypeWrapsPerModeForPydanticAndLeavesMsgspecUnchanged:
    def test_tool_mode_wraps_the_model_class_in_tool_output(self) -> None:
        plan = _pydantic_plan(mode="tool")

        marker = build_output_type(plan)

        assert isinstance(marker, ToolOutput)
        assert marker.output.__name__ == "StrictInvoiceModel"

    def test_native_mode_wraps_the_model_class_in_native_output(self) -> None:
        plan = _pydantic_plan(mode="native")

        marker = build_output_type(plan)

        assert isinstance(marker, NativeOutput)
        assert marker.outputs.__name__ == "StrictInvoiceModel"

    def test_absent_mode_returns_the_bare_class_not_none(self) -> None:
        plan = _pydantic_plan(mode=None)

        marker = build_output_type(plan)

        assert marker is not None
        assert getattr(marker, "__name__", None) == "StrictInvoiceModel"

    def test_a_msgspec_plan_still_wraps_a_structured_dict_class_not_the_schema_type(self) -> None:
        plan = make_plan(schema=STRICT_SCHEMA)
        inference = msgspec.structs.replace(plan.inference, output_mode="tool")
        plan = msgspec.structs.replace(plan, inference=inference)

        marker = build_output_type(plan)

        assert isinstance(marker, ToolOutput)
        assert issubclass(marker.output, dict)
        assert marker.output.__name__ != "StrictInvoiceModel"

    def test_a_msgspec_plan_with_no_mode_still_returns_none(self) -> None:
        plan = make_plan(schema=STRICT_SCHEMA)
        inference = msgspec.structs.replace(plan.inference, output_mode=None)
        plan = msgspec.structs.replace(plan, inference=inference)

        assert build_output_type(plan) is None

    def test_a_msgspec_plan_still_builds_a_structured_dict_marker_under_native(self) -> None:
        plan = make_plan(schema=STRICT_SCHEMA)
        inference = msgspec.structs.replace(plan.inference, output_mode="native")
        plan = msgspec.structs.replace(plan, inference=inference)

        marker = build_output_type(plan)

        assert isinstance(marker, NativeOutput)
        assert issubclass(marker.outputs, dict)
