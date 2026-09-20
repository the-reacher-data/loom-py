"""``PydanticAIEngine.native`` and ``native_agent``: the escape hatch out of loom.

Pins the two properties the hatch is worth having for: it hands over the very
agent the engine runs — not a copy, not a rebuild — and the dependency bundle
it comes with is built for the asking caller, so a run driven through it stays
inside the artefact's grants as that caller. The refusal pins the other half:
a foreign engine's native form is named as such rather than cast into ours.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from typing import Any, cast

import pytest
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart
from pydantic_ai.models import Model
from pydantic_ai.models.function import AgentInfo, FunctionModel

from loom.ai.abc import AgentHandle
from loom.ai.engines.pydantic_ai import NativeAgent, PydanticAIEngineProvider, native_agent
from loom.ai.engines.pydantic_ai._engine import PydanticAIEngine
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from tests.helpers.pydantic_ai_engine import make_plan

_IDENTITY = Identity(subject="caller")


@dataclass(frozen=True, slots=True)
class _Bundle:
    """A dependency bundle that remembers what it was built from."""

    identity: Identity
    state: Mapping[str, Any] | None


class _RecordingDeps:
    """Dependency factory handing back the bundle's own inputs."""

    def build(
        self,
        identity: Identity,
        container: LoomContainer,
        state: Mapping[str, Any] | None = None,
    ) -> object:
        """Return a bundle carrying this invocation's identity and state."""
        del container
        return _Bundle(identity=identity, state=state)


class _ForeignHandle:
    """A handle whose agent is served by some other engine."""

    def native(self, *, state: object | None = None) -> object:
        """Return a native form this package cannot name."""
        del state
        return object()


class _NativeHandle:
    """A handle over the real engine, standing in for a resolved marker."""

    def __init__(self, engine: PydanticAIEngine, identity: Identity) -> None:
        self._engine = engine
        self._identity = identity

    def native(self, *, state: object | None = None) -> object:
        """Return the engine's native form for this handle's caller."""
        return self._engine.native(identity=self._identity, state=state)


def _silent_model() -> Model:
    """A model that never has to answer: no run is started in this module."""

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        del messages, info
        return ModelResponse(parts=[TextPart(content="unused")])

    async def stream(messages: list[ModelMessage], info: AgentInfo) -> AsyncIterator[str]:
        del messages, info
        yield "unused"

    return FunctionModel(respond, stream_function=stream)


def _engine() -> PydanticAIEngine:
    model = _silent_model()
    provider = PydanticAIEngineProvider(model_resolver=lambda target: model)
    engine = provider.create_engine(make_plan(), deps=_RecordingDeps(), container=LoomContainer())
    assert isinstance(engine, PydanticAIEngine)
    return engine


def _as_handle(handle: object) -> AgentHandle[Any]:
    """Present a stub as the handle ``native_agent`` declares it takes."""
    return cast("AgentHandle[Any]", handle)


class TestTheHatchHandsOverTheRunningAgent:
    def test_the_agent_is_the_one_the_engine_itself_runs(self) -> None:
        engine = _engine()

        first = engine.native(identity=_IDENTITY)
        second = engine.native(identity=_IDENTITY)

        assert first.agent is second.agent

    def test_the_spend_caps_are_the_artefacts_own_projected_ones(self) -> None:
        engine = _engine()

        access = engine.native(identity=_IDENTITY)

        assert access.usage_limits.request_limit == make_plan().policies.max_requests


class TestTheBundleBelongsToTheAskingCaller:
    def test_the_bundle_carries_the_asking_identity(self) -> None:
        engine = _engine()
        other = Identity(subject="someone-else")

        mine = engine.native(identity=_IDENTITY)
        theirs = engine.native(identity=other)

        assert isinstance(mine.deps, _Bundle)
        assert isinstance(theirs.deps, _Bundle)
        assert mine.deps.identity == _IDENTITY
        assert theirs.deps.identity == other

    def test_each_call_builds_its_own_bundle(self) -> None:
        engine = _engine()

        first = engine.native(identity=_IDENTITY)
        second = engine.native(identity=_IDENTITY)

        assert first.deps is not second.deps

    def test_state_reaches_the_bundle_unchanged(self) -> None:
        engine = _engine()
        state = {"ticket": "INC-1"}

        access = engine.native(identity=_IDENTITY, state=state)

        assert isinstance(access.deps, _Bundle)
        assert access.deps.state == state


class TestTheTypedAccessor:
    def test_a_handle_on_this_engine_yields_the_engines_own_carrier(self) -> None:
        engine = _engine()

        access = native_agent(_as_handle(_NativeHandle(engine, _IDENTITY)))

        assert isinstance(access, NativeAgent)
        assert access.agent is engine.native(identity=_IDENTITY).agent

    def test_a_handle_on_another_engine_is_named_rather_than_cast(self) -> None:
        with pytest.raises(TypeError) as excinfo:
            native_agent(_as_handle(_ForeignHandle()))

        assert "not served by the pydantic-ai engine" in str(excinfo.value)

    def test_state_travels_through_the_accessor(self) -> None:
        access = native_agent(_as_handle(_NativeHandle(_engine(), _IDENTITY)), state={"a": 1})

        assert isinstance(access.deps, _Bundle)
        assert access.deps.state == {"a": 1}
