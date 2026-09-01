"""Public surface and engine-neutrality contract for ``loom.ai`` (T034, T035).

Pins two properties of the pillar's public API:

* ``loom.ai.__all__`` exposes exactly the engine-neutral surface — no private
  names, no engine or vendor types, and every listed name resolves.
* The ``AgentEngine`` protocol is conversation-free (FR-034): no message,
  history or conversation parameters; runs take a ``prompt`` and a
  keyword-only ``identity``, and ``run_stream`` is an async context manager.
"""

from __future__ import annotations

import inspect
from types import ModuleType

import pytest

import loom.ai
import loom.ai.declarative
from loom.ai.abc import AgentEngine

_REQUIRED_EXPORTS = frozenset(
    {
        "InferenceTarget",
        "AiConfig",
        "A2AConfig",
        "AgentEndpointConfig",
        "TextDeltaEvent",
        "ToolCallEvent",
        "ToolResultEvent",
        "ErrorEvent",
        "FinalEvent",
        "AgentEvent",
        "AgentResult",
        "AgentUsage",
        "HealthStatus",
        "AgentEngine",
        "AgentEngineProvider",
        "ToolsetFactory",
        "DepsFactory",
    }
)

_ENGINE_NAME_FRAGMENTS = ("Pydantic", "OpenAI", "Bedrock", "LangChain", "Fake")

# Names retired when ``mcp``/``a2a``/``skills`` collapsed onto one flat
# include/exclude filter.  A re-export would resurrect a vocabulary the
# artifact contract no longer has.
_RETIRED_EXPORTS = frozenset({"ToolFilter"})

_FORBIDDEN_RUN_PARAMS = frozenset(
    {"message", "messages", "history", "chat_history", "conversation"}
)


def _run_signature(method_name: str) -> inspect.Signature:
    """Signature of an ``AgentEngine`` protocol method."""
    return inspect.signature(getattr(AgentEngine, method_name))


class TestPublicExports:
    def test_all_contiene_la_superficie_publica_cuando_se_importa_loom_ai(self) -> None:
        """Every engine-neutral name of the fixed surface is exported."""
        assert set(loom.ai.__all__) >= _REQUIRED_EXPORTS

    def test_all_no_contiene_nombres_privados_cuando_se_importa_loom_ai(self) -> None:
        """No underscore-prefixed symbol leaks into the public surface."""
        assert [name for name in loom.ai.__all__ if name.startswith("_")] == []

    def test_all_no_contiene_tipos_de_motor_cuando_se_importa_loom_ai(self) -> None:
        """Vendor and engine types never appear in the neutral surface (FR-034)."""
        leaked = [
            name
            for name in loom.ai.__all__
            if any(fragment in name for fragment in _ENGINE_NAME_FRAGMENTS)
        ]

        assert leaked == []

    def test_todo_nombre_de_all_resuelve_cuando_se_importa_loom_ai(self) -> None:
        """``__all__`` never advertises a name the module cannot deliver."""
        missing = [name for name in loom.ai.__all__ if not hasattr(loom.ai, name)]

        assert missing == []

    def test_all_no_contiene_nombres_retirados_cuando_se_importa_loom_ai(self) -> None:
        """``ToolFilter`` was retired with the nested filter; it must not come back."""
        assert set(loom.ai.__all__) & _RETIRED_EXPORTS == set()

    @pytest.mark.parametrize("module", [loom.ai, loom.ai.declarative], ids=["ai", "declarative"])
    def test_los_nombres_retirados_no_son_alcanzables_cuando_se_importa_el_modulo(
        self,
        module: ModuleType,
    ) -> None:
        """Absence from ``__all__`` is not enough: the attribute must be gone too."""
        reachable = [name for name in _RETIRED_EXPORTS if hasattr(module, name)]

        assert reachable == []

    def test_loom_ai_no_exporta_identity_cuando_se_importa(self) -> None:
        """Identity comes from the caller, never from the AI pillar (FR-043)."""
        identity_like = [name for name in loom.ai.__all__ if "identity" in name.lower()]

        assert identity_like == []


class TestAgentEngineProtocol:
    @pytest.mark.parametrize("method_name", ["run", "run_stream"])
    def test_no_expone_parametros_de_conversacion_cuando_se_inspecciona(
        self,
        method_name: str,
    ) -> None:
        """Runs are single-shot: no message, history or conversation params (FR-034)."""
        parameters = set(_run_signature(method_name).parameters)

        assert parameters & _FORBIDDEN_RUN_PARAMS == set()

    @pytest.mark.parametrize("method_name", ["run", "run_stream"])
    def test_identity_es_keyword_only_cuando_se_inspecciona(
        self,
        method_name: str,
    ) -> None:
        """Every run takes the caller identity as an explicit keyword."""
        identity = _run_signature(method_name).parameters["identity"]

        assert identity.kind is inspect.Parameter.KEYWORD_ONLY

    def test_run_stream_devuelve_un_async_context_manager_cuando_se_inspecciona(
        self,
    ) -> None:
        """``run_stream`` is annotated as ``AbstractAsyncContextManager`` (R-008)."""
        annotations = getattr(AgentEngine.run_stream, "__annotations__", {})

        assert "AbstractAsyncContextManager" in str(annotations.get("return"))
