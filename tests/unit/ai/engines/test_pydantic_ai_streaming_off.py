"""A binding declaring ``streaming: false`` asks the provider for each answer whole.

The run is still walked as events -- tool calls, tool results, the terminal
one -- but the model is never asked to stream, so a model whose streamed tool
calls arrive damaged can still be served.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass

import msgspec
from pydantic_ai.messages import (
    ModelMessage,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
)
from pydantic_ai.models.function import AgentInfo, DeltaToolCall, DeltaToolCalls, FunctionModel
from pydantic_ai.toolsets import FunctionToolset

from loom.ai.abc import (
    ErrorEvent,
    FinalEvent,
    TextDeltaEvent,
    ToolCallEvent,
    ToolResultEvent,
)
from loom.ai.compiler import CompiledPythonCapability
from loom.ai.inference import InferenceTarget
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from tests.helpers.pydantic_ai_engine import build_engine, encode, make_plan

_IDENTITY = Identity(subject="caller")
_ANSWER: Mapping[str, str] = {"answer": "ok"}
_WHOLE = InferenceTarget(provider="bedrock", model="a-model", region="eu-west-1", streaming=False)
_STREAMED = msgspec.structs.replace(_WHOLE, streaming=True)


def _whole_only_model(payload: bytes) -> FunctionModel:
    """A model that answers only when asked whole: it declares no stream function."""

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[ToolCallPart(info.output_tools[0].name, payload.decode())])

    return FunctionModel(respond)


def _damaged_stream_model(payload: bytes) -> FunctionModel:
    """A model whose stream drops the first two characters of every tool call."""
    text = payload.decode()

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[ToolCallPart(info.output_tools[0].name, text)])

    async def stream(
        messages: list[ModelMessage], info: AgentInfo
    ) -> AsyncIterator[DeltaToolCalls]:
        name = info.output_tools[0].name
        damaged = DeltaToolCall(name=name, json_args=text[2:], tool_call_id="c")
        yield {0: damaged}

    return FunctionModel(respond, stream_function=stream)


def _lookup(city: str) -> str:
    return f"{city}: sunny"


@dataclass(frozen=True)
class _Bundle:
    """The attributes a capability call is gated on."""

    identity: Identity
    container: LoomContainer


class _CallerDeps:
    """A factory whose bundle lets the agent reach its tools."""

    def build(self, identity: Identity, container: LoomContainer, state: object = None) -> object:
        return _Bundle(identity=identity, container=container)


def _talking_model(payload: bytes) -> FunctionModel:
    """A model that says something before answering."""

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        said = TextPart("lo he mirado")
        return ModelResponse(
            parts=[said, ToolCallPart(info.output_tools[0].name, payload.decode())]
        )

    return FunctionModel(respond)


def _tool_then_answer_model(payload: bytes) -> FunctionModel:
    """A model that calls ``lookup`` first and answers on the turn after."""

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        returned = any(
            isinstance(part, ToolReturnPart)
            for message in messages
            for part in getattr(message, "parts", ())
        )
        if not returned:
            return ModelResponse(parts=[ToolCallPart("_lookup", {"city": "Vigo"})])
        return ModelResponse(parts=[ToolCallPart(info.output_tools[0].name, payload.decode())])

    return FunctionModel(respond)


def test_a_binding_streams_unless_it_says_otherwise() -> None:
    assert InferenceTarget(provider="openai", model="a-model").streaming is True


async def test_the_answer_comes_back_whole_without_the_model_ever_streaming() -> None:
    engine = build_engine(make_plan(inference=_WHOLE), _whole_only_model(encode(_ANSWER)))

    result = await engine.run("assess", identity=_IDENTITY)

    assert result.output == _ANSWER


async def test_the_text_of_a_whole_answer_reaches_the_caller_as_one_delta() -> None:
    """Whole or streamed, a caller watching the run still sees what the model said."""
    engine = build_engine(make_plan(inference=_WHOLE), _talking_model(encode(_ANSWER)))

    async with engine.run_stream("assess", identity=_IDENTITY) as events:
        seen = [event async for event in events]

    assert [event for event in seen if isinstance(event, TextDeltaEvent)] == [
        TextDeltaEvent(text="lo he mirado")
    ]
    assert isinstance(seen[-1], FinalEvent)
    assert seen[-1].output == _ANSWER


async def _terminal(engine: object) -> object:
    """The last event of a streamed run: what a supervised caller finally sees."""
    async with engine.run_stream("assess", identity=_IDENTITY) as events:  # type: ignore[attr-defined]
        return [event async for event in events][-1]


async def test_a_damaged_stream_is_what_asking_whole_is_for() -> None:
    model = _damaged_stream_model(encode(_ANSWER))

    streamed = await _terminal(build_engine(make_plan(inference=_STREAMED), model))
    whole = await _terminal(build_engine(make_plan(inference=_WHOLE), model))

    assert isinstance(streamed, ErrorEvent)
    assert isinstance(whole, FinalEvent)
    assert whole.output == _ANSWER


async def test_tool_calls_and_their_results_still_reach_the_caller() -> None:
    capability = CompiledPythonCapability(
        factory_ref="tests:factory",
        factory=lambda context: FunctionToolset([_lookup], max_retries=0),
    )
    plan = msgspec.structs.replace(make_plan(inference=_WHOLE), capabilities=(capability,))
    engine = build_engine(plan, _tool_then_answer_model(encode(_ANSWER)), deps=_CallerDeps())

    async with engine.run_stream("weather?", identity=_IDENTITY) as events:
        seen = [event async for event in events]

    assert [type(event) for event in seen] == [ToolCallEvent, ToolResultEvent, FinalEvent]
