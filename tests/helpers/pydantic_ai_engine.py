"""Scripted pydantic-ai engines, shared by the contract, unit and integration tests.

Every engine built here talks to a ``FunctionModel``: no network, no
credentials, no tokens. The plan is a real compiled plan and the engine is the
real adapter — only the model object is scripted, which is the seam
:class:`~loom.ai.engines.pydantic_ai.PydanticAIEngineProvider` exposes for
exactly this purpose.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Mapping
from typing import Any

import msgspec
from pydantic_ai.messages import ModelMessage, ModelResponse, ToolCallPart
from pydantic_ai.models import Model
from pydantic_ai.models.function import AgentInfo, DeltaToolCall, DeltaToolCalls, FunctionModel

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


def build_engine(plan: AgentPlan, model: Model) -> AgentEngine:
    """Build the real adapter over a scripted model."""
    provider = PydanticAIEngineProvider(model_resolver=lambda target: model)
    return provider.create_engine(plan, deps=NullDeps(), container=LoomContainer())


def encode(payload: Mapping[str, Any]) -> bytes:
    """Encode a scripted answer; the test writes bytes, the engine reads them."""
    return msgspec.json.encode(dict(payload))
