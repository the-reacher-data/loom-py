"""The engine maps loom tool names to provider classes and registers them."""

from __future__ import annotations

from typing import cast

import pytest

from loom.ai.compiler._plan import CompiledNativeCapability
from loom.ai.declarative import PolicySpec
from loom.ai.engines.pydantic_ai._capabilities import build_capabilities, build_toolsets
from loom.ai.engines.pydantic_ai._native import TOOL_CLASSES, supported_native_tools
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.inference import InferenceTarget
from loom.ai.registry import engine_native_tool_support
from loom.core.di import LoomContainer


class _Plan:
    """Stand-in carrying only what the builder reads."""

    def __init__(self, *tools: str) -> None:
        self.name = "searcher"
        self.policies = PolicySpec()
        self.capabilities = tuple(CompiledNativeCapability(tool=tool) for tool in tools)


def _bedrock_admits_web_search() -> bool:
    """Whether the installed Bedrock class has gained provider-run web search."""
    from pydantic_ai.models.bedrock import BedrockConverseModel
    from pydantic_ai.native_tools import WebSearchTool

    return WebSearchTool in BedrockConverseModel.supported_native_tools()


_BEDROCK_ADMITS_WEB_SEARCH = _bedrock_admits_web_search()


def test_el_mapa_del_motor_cubre_exactamente_los_nombres_del_artefacto() -> None:
    """Every name the artifact may declare indexes a class, and no other does."""
    from loom.ai.declarative import NATIVE_TOOLS

    assert set(TOOL_CLASSES) == set(NATIVE_TOOLS)


def test_cada_clase_declara_el_mismo_nombre_que_la_indexa() -> None:
    """The engine's own identifier for each tool matches the loom name."""
    assert all(TOOL_CLASSES[name]().kind == name for name in TOOL_CLASSES)


@pytest.mark.skipif(
    _BEDROCK_ADMITS_WEB_SEARCH,
    reason="bedrock ya admite web_search: revisar la tabla de docs/ai/artifacts.md",
)
def test_bedrock_no_admite_busqueda_web_y_si_ejecucion_de_codigo() -> None:
    """The truth comes from the model class, not from a table in loom."""
    target = InferenceTarget(provider="bedrock", model="anthropic.claude-x", region="eu-west-1")

    admitted = supported_native_tools(target)

    assert "code_execution" in admitted
    assert "web_search" not in admitted


@pytest.mark.parametrize("provider", ["openai", "anthropic"])
def test_openai_y_anthropic_admiten_busqueda_web(provider: str) -> None:
    """Both vendor classes advertise web search at class level."""
    target = InferenceTarget(provider=provider, model="a-model")

    assert "web_search" in supported_native_tools(target)


def test_falla_nombrando_los_proveedores_cuando_el_vendor_es_desconocido() -> None:
    """An unknown provider is refused before any tool is considered."""
    with pytest.raises(AgentCompilationError) as failure:
        supported_native_tools(InferenceTarget(provider="unheard-of", model="x"))

    assert failure.value.issues[0].code is AgentErrorCode.PROVIDER_UNKNOWN


def test_construye_una_capacidad_por_concesion_en_el_orden_del_plan() -> None:
    """Order is the artifact's, and each grant becomes exactly one capability."""
    from pydantic_ai.native_tools import CodeExecutionTool, WebSearchTool

    built = build_capabilities(_Plan("web_search", "code_execution"), LoomContainer())

    assert [type(capability.tool) for capability in built] == [WebSearchTool, CodeExecutionTool]


def test_no_construye_nada_cuando_el_plan_no_concede_ninguna() -> None:
    """A plan without native grants leaves the engine call untouched."""
    assert build_capabilities(_Plan(), LoomContainer()) == ()


def test_el_provider_expone_el_oraculo_que_el_registro_lee() -> None:
    """The bootstrap finds the oracle through the documented handshake."""
    from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider

    support = engine_native_tool_support(PydanticAIEngineProvider())

    assert support is not None
    assert "web_search" in support(InferenceTarget(provider="anthropic", model="a-model"))


def test_el_registro_devuelve_none_cuando_el_motor_no_lo_aporta() -> None:
    """An engine that serves no native grant declares no oracle."""

    class _Engine:
        pass

    assert engine_native_tool_support(_Engine()) is None


def test_el_grant_native_no_produce_toolset_y_llega_como_capacidad() -> None:
    """A native grant is served as an engine capability, never as a toolset."""

    class _NativePlan:
        name = "searcher"
        policies = PolicySpec()
        capabilities = (CompiledNativeCapability(tool="web_search"),)

    plan = _NativePlan()

    assert build_toolsets(plan, LoomContainer()) == ()
    assert len(build_capabilities(plan, LoomContainer())) == 1


def test_el_registro_retira_native_cuando_el_motor_no_aporta_oraculo(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An engine that cannot check a grant does not get to serve the kind."""
    import logging

    from loom.ai.registry import engine_supported_kinds

    class _Engine:
        def supported_capability_kinds(self) -> frozenset[str]:
            return frozenset({"sql", "native"})

    with caplog.at_level(logging.WARNING, logger="loom.ai.registry"):
        kinds = engine_supported_kinds(_Engine(), "third-party")

    assert kinds == frozenset({"sql"})
    assert "third-party" in caplog.text


def test_el_registro_conserva_native_cuando_el_motor_lo_aporta() -> None:
    """The real engine keeps the kind, because it supplies the oracle."""
    from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider
    from loom.ai.registry import engine_supported_kinds

    assert "native" in engine_supported_kinds(PydanticAIEngineProvider(), "pydantic-ai")


def test_los_kinds_anunciados_son_exactamente_los_de_la_tabla() -> None:
    """The adapter cannot announce a kind it has no builder for, or hide one it has."""
    from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider
    from loom.ai.engines.pydantic_ai._capabilities import _KINDS

    announced = PydanticAIEngineProvider().supported_capability_kinds()

    assert announced == frozenset(compiled.kind for compiled in _KINDS)


def test_falla_nombrando_el_kind_cuando_un_grant_no_tiene_builder() -> None:
    """A compiled grant of an unserved kind is refused, not silently dropped."""
    from loom.ai.compiler._plan import CompiledCapability
    from loom.ai.engines.pydantic_ai._capabilities import build_capabilities
    from loom.ai.errors import AgentCompilationError

    class _Unserved:
        kind = "telepathy"

    class _PlanWithUnserved:
        name = "searcher"
        policies = PolicySpec()
        capabilities = (cast("CompiledCapability", _Unserved()),)

    with pytest.raises(AgentCompilationError, match="telepathy"):
        build_capabilities(_PlanWithUnserved(), LoomContainer())  # type: ignore[arg-type]
