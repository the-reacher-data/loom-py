"""Instructions and description are projected as literals (AC1, AC2).

The engine types both fields as ``TemplateStr | str``, a union discriminated by
the presence of ``{{`` in the text, whose template branch imports a templating
package a default install does not carry. Loom therefore builds the engine spec
without them and passes them as keyword arguments, so an artifact whose prompt
mentions braces builds and reaches the model byte for byte.

The A2A card is not on this path: it reads ``plan.description`` directly
(``loom.ai.a2a.card``) and was never affected, so nothing here covers it.
"""

from __future__ import annotations

import sys
from typing import Any

import pytest
from msgspec import structs
from pydantic_ai.messages import ModelMessage, ModelRequest
from pydantic_ai.models.test import TestModel

from loom.ai.compiler._plan import AgentPlan
from loom.ai.engines.pydantic_ai._spec import build_agent_spec
from loom.ai.engines.pydantic_ai.provider import PydanticAIEngineProvider
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from tests.helpers.pydantic_ai_engine import (
    STRICT_SCHEMA,
    NullDeps,
    build_engine,
    encode,
    make_plan,
    recording_model,
)

_INSTRUCTIONS = "report {{incident}} using the {{playbook}} verbatim"
"""Instructions whose braces select the engine's template branch."""

_DESCRIPTION = "answers about {{incident}}"
"""Description whose braces select the same branch."""

_TEMPLATE_PACKAGE = "pydantic_handlebars"
"""Package the template branch imports; absent from a default install."""

_PROMPT = "what happened?"
_IDENTITY = Identity(subject="caller")


def _plan_with_braces() -> AgentPlan:
    """A plan whose instructions and description both contain ``{{``."""
    return structs.replace(
        make_plan(schema=STRICT_SCHEMA),
        instructions=_INSTRUCTIONS,
        description=_DESCRIPTION,
    )


def _model_instructions(messages: list[ModelMessage]) -> list[str | None]:
    return [message.instructions for message in messages if isinstance(message, ModelRequest)]


class _ImportBlocker:
    """Meta-path finder refusing one package and recording every request for it.

    Attributes:
        asked: Full module names the import system asked this finder about.
    """

    def __init__(self, package: str) -> None:
        self._package = package
        self.asked: list[str] = []

    def find_spec(self, fullname: str, path: object = None, target: object = None) -> None:
        """Refuse the guarded package, and stay out of the way for anything else."""
        if fullname != self._package and not fullname.startswith(f"{self._package}."):
            return None
        self.asked.append(fullname)
        raise ImportError(f"{fullname} is blocked by this test")


class _FromSpecRecorder:
    """Stands in for ``Agent.from_spec`` and keeps the keywords it received."""

    def __init__(self) -> None:
        self.kwargs: dict[str, Any] = {}

    def __call__(self, spec: object, **kwargs: Any) -> object:
        self.kwargs = kwargs
        return object()


@pytest.fixture
def from_spec(monkeypatch: pytest.MonkeyPatch) -> _FromSpecRecorder:
    """Patch ``Agent.from_spec`` in the provider's module and record its keywords."""
    recorder = _FromSpecRecorder()
    monkeypatch.setattr("loom.ai.engines.pydantic_ai.provider.Agent.from_spec", recorder)
    return recorder


class TestLiteralProjection:
    def test_el_spec_del_motor_no_lleva_instructions_ni_description(self) -> None:
        """The template-typed fields never reach the engine's spec type."""
        spec = build_agent_spec(_plan_with_braces())

        assert spec.instructions is None
        assert spec.description is None

    async def test_el_agente_se_construye_y_el_modelo_ve_las_instructions_verbatim(self) -> None:
        """Braces build an agent and arrive at the model byte for byte."""
        seen: list[list[ModelMessage]] = []
        engine = build_engine(_plan_with_braces(), recording_model(encode({"answer": "ok"}), seen))

        await engine.run(_PROMPT, identity=_IDENTITY, conversation=None)

        assert _model_instructions(seen[0]) == [_INSTRUCTIONS]

    def test_las_dos_llegan_a_from_spec_como_argumentos(self, from_spec: _FromSpecRecorder) -> None:
        """The provider hands both texts to the agent factory, unchanged."""
        plan = _plan_with_braces()
        provider = PydanticAIEngineProvider(model_resolver=lambda target: TestModel())

        provider.create_engine(plan, deps=NullDeps(), container=LoomContainer())

        assert from_spec.kwargs["instructions"] == _INSTRUCTIONS
        assert from_spec.kwargs["description"] == _DESCRIPTION

    def test_no_importa_el_paquete_de_plantillas_al_construir_el_agente(self) -> None:
        """The build must not reach for the templating package, installed or not.

        Asserting the package is merely absent from ``sys.modules`` cannot fail
        where it is not installed, which is every default environment. So the
        import is *blocked* instead: a finder ahead of the real ones raises for
        that name, and the agent still builds. Should loom go back to putting
        the braces inside the engine spec, the template branch would ask for the
        package, the blocker would answer, and the build would fail here — in
        an environment that has it as well as in one that does not.
        """
        blocker = _ImportBlocker(_TEMPLATE_PACKAGE)
        sys.meta_path.insert(0, blocker)
        try:
            build_engine(_plan_with_braces(), TestModel())
        finally:
            sys.meta_path.remove(blocker)

        assert blocker.asked == []
