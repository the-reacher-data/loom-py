"""The engine maps loom tool names to provider classes and registers them."""

from __future__ import annotations

import pytest

from loom.ai.compiler._plan import CompiledNativeCapability
from loom.ai.engines.pydantic_ai._native import (
    NATIVE_TOOL_NAMES,
    build_native_capabilities,
    supported_native_tools,
)
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.inference import InferenceTarget
from loom.ai.registry import engine_native_tool_support


class _Plan:
    """Stand-in carrying only what the builder reads."""

    def __init__(self, *tools: str) -> None:
        self.capabilities = tuple(CompiledNativeCapability(tool=tool) for tool in tools)


def test_el_vocabulario_del_motor_cubre_los_nombres_del_artefacto() -> None:
    """Every name the artifact may declare has a class behind it."""
    from loom.ai.declarative import NATIVE_TOOLS

    assert set(NATIVE_TOOL_NAMES) == set(NATIVE_TOOLS)


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

    built = build_native_capabilities(_Plan("web_search", "code_execution"))

    assert [type(capability.tool) for capability in built] == [WebSearchTool, CodeExecutionTool]


def test_no_construye_nada_cuando_el_plan_no_concede_ninguna() -> None:
    """A plan without native grants leaves the engine call untouched."""
    assert build_native_capabilities(_Plan()) == ()


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
