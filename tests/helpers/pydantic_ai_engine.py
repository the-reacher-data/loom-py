"""Scripted pydantic-ai engines, shared by the contract, unit and integration tests.

Every engine built here talks to a ``FunctionModel``: no network, no
credentials, no tokens. The plan is a real compiled plan and the engine is the
real adapter — only the model object is scripted, which is the seam
:class:`~loom.ai.engines.pydantic_ai.PydanticAIEngineProvider` exposes for
exactly this purpose.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import msgspec
from pydantic_ai import RunContext
from pydantic_ai.messages import (
    ModelMessage,
    ModelResponse,
    ModelResponseStreamEvent,
    ToolCallPart,
)
from pydantic_ai.models import Model, ModelRequestParameters, StreamedResponse
from pydantic_ai.models.function import AgentInfo, DeltaToolCall, DeltaToolCalls, FunctionModel
from pydantic_ai.settings import ModelSettings
from pydantic_ai.usage import RequestUsage

from loom.ai.abc import AgentEngine
from loom.ai.compiler._plan import AgentPlan, CompiledOutput
from loom.ai.compiler.phases._output import compile_output
from loom.ai.declarative import JsonSchemaOutput, PolicySpec
from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider
from loom.ai.inference import InferenceTarget
from loom.core.di import LoomContainer
from loom.core.identity import Identity

OPEN_OBJECT_SCHEMA: Mapping[str, Any] = {"type": "object"}
"""Schema of an object with no declared properties: decodes to a ``dict``."""

_SCRIPTED_NAME = "scripted"
"""Model, provider and system name of :class:`ScriptedUsageModel`."""

STRICT_SCHEMA: Mapping[str, Any] = {
    "type": "object",
    "properties": {"answer": {"type": "string"}},
    "required": ["answer"],
}
"""Schema whose compiled decoder rejects an unknown field (invariant 5)."""


class NullDeps:
    """Dependency factory the contract plans need: no service, no state."""

    def build(self, identity: Identity, container: LoomContainer) -> object:
        """Return the empty dependency bundle.

        Args:
            identity: Verified caller of this invocation.
            container: Application container.

        Returns:
            ``None``: a pure-language agent depends on nothing.
        """
        return None


def compiled_output(schema: Mapping[str, Any]) -> CompiledOutput:
    """Compile ``schema`` through the real output phase."""
    output, issues = compile_output(JsonSchemaOutput(schema=schema), "contract")
    assert output is not None, issues
    return output


def make_plan(*, schema: Mapping[str, Any] = OPEN_OBJECT_SCHEMA, retries: int = 0) -> AgentPlan:
    """Build a compiled plan for a pure-language agent."""
    return AgentPlan(
        name="contract",
        description="contract agent",
        instructions="answer the question",
        spec_version=1,
        inference=InferenceTarget(provider="openai", model="gpt-5.2"),
        output=compiled_output(schema),
        policies=PolicySpec(retries=retries),
        metadata={},
    )


def answering_model(payload: bytes) -> Model:
    """A model that answers ``payload`` verbatim, in both run modes."""
    text = payload.decode()

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        tool = info.output_tools[0].name
        return ModelResponse(parts=[ToolCallPart(tool_name=tool, args=text)])

    async def stream(
        messages: list[ModelMessage], info: AgentInfo
    ) -> AsyncIterator[DeltaToolCalls]:
        tool = info.output_tools[0].name
        yield {0: DeltaToolCall(name=tool, json_args=text, tool_call_id="contract-call")}

    return FunctionModel(respond, stream_function=stream)


def failing_model(failure: Callable[[], Exception]) -> Model:
    """A model that raises the scripted failure, in both run modes."""

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        raise failure()

    async def stream(
        messages: list[ModelMessage], info: AgentInfo
    ) -> AsyncIterator[DeltaToolCalls]:
        raise failure()
        yield {}  # pragma: no cover - unreachable, makes the function a generator

    return FunctionModel(respond, stream_function=stream)


@dataclass
class _ScriptedUsageStream(StreamedResponse):
    """Streamed output-tool call whose usage is scripted, not estimated.

    ``FunctionModel`` estimates the usage of a streamed run from the deltas it
    saw, so it cannot express a run that reports a cost, a cache split or a
    counter the engine never declared. This response reports exactly what the
    test scripted, which is what makes the streaming and non-streaming usage
    of one run comparable.
    """

    _payload: str = ""
    _scripted: RequestUsage = field(default_factory=RequestUsage)
    _timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        self._usage = self._scripted

    async def _get_event_iterator(self) -> AsyncIterator[ModelResponseStreamEvent]:
        tool = self.model_request_parameters.output_tools[0].name
        event = self._parts_manager.handle_tool_call_delta(
            vendor_part_id=0, tool_name=tool, args=self._payload, tool_call_id="scripted-call"
        )
        if event is not None:
            yield event

    @property
    def model_name(self) -> str:
        """Return the scripted model name."""
        return _SCRIPTED_NAME

    @property
    def provider_name(self) -> str:
        """Return the scripted provider name."""
        return _SCRIPTED_NAME

    @property
    def provider_url(self) -> str:
        """Return a URL no test ever reaches."""
        return "https://scripted.invalid"

    @property
    def timestamp(self) -> datetime:
        """Return when this response was built."""
        return self._timestamp


class ScriptedUsageModel(Model):
    """A model answering the same payload and the same usage in both run modes.

    Args:
        payload: JSON arguments of the output-tool call, as bytes on the wire.
        usage: Accounting reported for the single request of the run.
    """

    def __init__(self, payload: bytes, usage: RequestUsage) -> None:
        super().__init__()
        self._payload = payload.decode()
        self._scripted = usage

    @property
    def model_name(self) -> str:
        """Return the scripted model name."""
        return _SCRIPTED_NAME

    @property
    def system(self) -> str:
        """Return the scripted provider system."""
        return _SCRIPTED_NAME

    async def request(
        self,
        messages: list[ModelMessage],
        model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters,
    ) -> ModelResponse:
        """Answer the payload as an output-tool call carrying the scripted usage."""
        del messages, model_settings
        tool = model_request_parameters.output_tools[0].name
        return ModelResponse(
            parts=[ToolCallPart(tool_name=tool, args=self._payload)], usage=self._scripted
        )

    @asynccontextmanager
    async def request_stream(
        self,
        messages: list[ModelMessage],
        model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters,
        run_context: RunContext[Any] | None = None,
    ) -> AsyncIterator[StreamedResponse]:
        """Stream the same answer and the same usage the request mode reports."""
        del messages, model_settings, run_context
        yield _ScriptedUsageStream(
            model_request_parameters=model_request_parameters,
            _payload=self._payload,
            _scripted=self._scripted,
        )


def build_engine(plan: AgentPlan, model: Model) -> AgentEngine:
    """Build the real adapter over a scripted model."""
    provider = PydanticAIEngineProvider(model_resolver=lambda target: model)
    return provider.create_engine(plan, deps=NullDeps(), container=LoomContainer())


def encode(payload: Mapping[str, Any]) -> bytes:
    """Encode a scripted answer; the test writes bytes, the engine reads them."""
    return msgspec.json.encode(dict(payload))
