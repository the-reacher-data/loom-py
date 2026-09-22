"""``provider_details``: what the provider said about the answer, beyond the answer.

A decision model answers a typed output with a probability per field; a
language model may report a finish reason. Whatever it is, the engine hands it
over verbatim on ``AgentResult`` and on the ``final`` event, read off the
run's last response, and ``None`` when the provider reported nothing.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from typing import Any

import msgspec
import pytest
from pydantic_ai.messages import ModelMessage, ModelResponse, ToolCallPart
from pydantic_ai.models.function import AgentInfo, DeltaToolCall, DeltaToolCalls, FunctionModel

from loom.ai.abc import FinalEvent
from loom.ai.inference import InferenceTarget
from loom.core.identity import Identity
from tests.helpers.pydantic_ai_engine import build_engine, encode, make_plan

_IDENTITY = Identity(subject="caller")
_PROMPT = "judge this"
_ANSWER = encode({"answer": "42"})
_DETAILS: Mapping[str, Any] = {
    "confidence": {"answer": 0.91},
    "probabilities": {"answer": {"42": 0.91, "41": 0.09}},
}
_WHOLE = InferenceTarget(provider="bedrock", model="a-model", region="eu-west-1", streaming=False)


def _model(details: Mapping[str, Any] | None) -> FunctionModel:
    """A model answering ``_ANSWER`` whole or streamed, reporting *details* on the whole path."""

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        tool = info.output_tools[0].name
        return ModelResponse(
            parts=[ToolCallPart(tool_name=tool, args=_ANSWER.decode())],
            provider_details=dict(details) if details is not None else None,
        )

    async def stream(
        messages: list[ModelMessage], info: AgentInfo
    ) -> AsyncIterator[DeltaToolCalls]:
        tool = info.output_tools[0].name
        yield {0: DeltaToolCall(name=tool, json_args=_ANSWER.decode(), tool_call_id="call")}

    return FunctionModel(respond, stream_function=stream)


async def _final(engine: Any) -> FinalEvent:
    async with engine.run_stream(_PROMPT, identity=_IDENTITY) as events:
        terminal = [event async for event in events][-1]
    assert isinstance(terminal, FinalEvent)
    return terminal


class TestProviderDetailsReachTheCaller:
    async def test_run_carries_what_the_provider_reported_about_the_answer(self) -> None:
        """``AgentResult.provider_details`` is the last response's own, verbatim."""
        engine = build_engine(make_plan(), _model(_DETAILS))

        result = await engine.run(_PROMPT, identity=_IDENTITY)

        assert result.output == {"answer": "42"}
        assert result.provider_details == _DETAILS

    async def test_the_final_event_carries_it_on_a_whole_answer_binding(self) -> None:
        """A ``streaming: false`` binding is served whole, so the stream's ``final`` has it too."""
        plan = msgspec.structs.replace(make_plan(), inference=_WHOLE)
        engine = build_engine(plan, _model(_DETAILS))

        final = await _final(engine)

        assert final.output == {"answer": "42"}
        assert final.provider_details == _DETAILS

    @pytest.mark.parametrize("streamed", [False, True])
    async def test_it_is_none_when_the_provider_reported_nothing(self, streamed: bool) -> None:
        """No detail is ``None``, never an empty mapping: absent, not empty."""
        plan = make_plan() if streamed else msgspec.structs.replace(make_plan(), inference=_WHOLE)
        engine = build_engine(plan, _model(None))

        result = await engine.run(_PROMPT, identity=_IDENTITY)
        final = await _final(engine)

        assert result.provider_details is None
        assert final.provider_details is None
