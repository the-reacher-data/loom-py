"""``python`` capability at build: how the engine calls the application factory.

The factory is application code the artifact names; the engine calls it once at
start-up. These tests pin the call shape — what arrives as the first positional
and how the declared ``params`` arrive — with a recording factory, no model and
no network.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from pydantic_ai.toolsets import FunctionToolset

from loom.ai.compiler._plan import AgentPlan, CompiledPythonCapability
from loom.ai.declarative import PolicySpec
from loom.ai.engines.pydantic_ai import _capabilities
from loom.ai.engines.pydantic_ai._mcp import SharedMcpToolsets
from loom.ai.inference import InferenceTarget
from loom.core.di import LoomContainer
from tests.helpers.pydantic_ai_engine import OPEN_OBJECT_SCHEMA, compiled_output

FACTORY_REF = "myapp.tools.geo:build_geo_toolset"
"""Reference the artifact named; the compiler resolved it to the callable below."""

PARAMS: Mapping[str, Any] = {"max_results": 3, "radius_km": 25}
"""Keyword arguments the artifact declared under ``params``."""


@dataclass
class RecordingFactory:
    """Factory that records every call it receives.

    Attributes:
        calls: ``(positional arguments, keyword arguments)`` per invocation.
    """

    calls: list[tuple[tuple[object, ...], Mapping[str, Any]]] = field(default_factory=list)

    def __call__(self, *args: object, **kwargs: Any) -> object:
        """Record the call and return an engine toolset."""
        self.calls.append((args, kwargs))
        return FunctionToolset()


def make_plan(capability: CompiledPythonCapability) -> AgentPlan:
    """Build a compiled plan granting ``capability`` and nothing else."""
    return AgentPlan(
        name="geo-agent",
        description="looks up service points",
        instructions="answer the question",
        spec_version=1,
        inference=InferenceTarget(provider="openai", model="gpt-5.2"),
        output=compiled_output(OPEN_OBJECT_SCHEMA),
        capabilities=(capability,),
        policies=PolicySpec(retries=0, tool_timeout_ms=5000),
        metadata={},
    )


class TestFactoryCallShape:
    """The factory is called once with the container first and ``params`` as kwargs."""

    def test_factory_receives_the_container_and_the_params_at_build(self) -> None:
        """``factory(container, **params)``, exactly once."""
        factory = RecordingFactory()
        capability = CompiledPythonCapability(
            factory_ref=FACTORY_REF, factory=factory, params=PARAMS
        )
        container = LoomContainer()

        _capabilities.build_toolsets(make_plan(capability), container, mcp=SharedMcpToolsets())

        assert len(factory.calls) == 1
        args, kwargs = factory.calls[0]
        assert args[0] is container
        assert kwargs == PARAMS

    def test_factory_receives_only_the_container_without_params(self) -> None:
        """Without ``params`` the factory is called with the first positional alone."""
        factory = RecordingFactory()
        capability = CompiledPythonCapability(factory_ref=FACTORY_REF, factory=factory)
        container = LoomContainer()

        _capabilities.build_toolsets(make_plan(capability), container, mcp=SharedMcpToolsets())

        assert factory.calls == [((container,), {})]
