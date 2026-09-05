"""A ``native`` grant is checked against the model bound to the agent's role."""

from __future__ import annotations

import pytest

from loom.ai.compiler import CompiledNativeCapability
from loom.ai.compiler.phases._capabilities import compile_capabilities
from loom.ai.config import AiConfig
from loom.ai.declarative import AgentSpecV1, JsonSchemaOutput, NativeCapability
from loom.ai.errors import AgentCompilationError, AgentErrorCode, provider_not_installed
from loom.ai.inference import InferenceTarget
from loom.core.use_case.registry import UseCaseRegistry

_KINDS = frozenset({"native"})
_SCHEMA = {"type": "object", "additionalProperties": False, "properties": {}}


def _spec(*tools: str) -> AgentSpecV1:
    """An artifact granting one ``native`` capability per tool named."""
    return AgentSpecV1(
        spec_version=1,
        name="searcher",
        description="Answers with the provider's own tools.",
        instructions="Use the granted provider tools.",
        output=JsonSchemaOutput(schema=dict(_SCHEMA)),
        capabilities=tuple(NativeCapability(tool=tool) for tool in tools),
    )


def _compile(spec: AgentSpecV1, **kwargs: object) -> tuple[object, list[object]]:
    """Compile the capabilities of *spec* with the given deployment inputs."""
    return compile_capabilities(
        spec,
        component="agents/searcher/agent.yaml",
        config=AiConfig(engine="pydantic-ai", specs=(), models={}),
        registry=UseCaseRegistry.build([]),
        sql=None,
        supported_kinds=_KINDS,
        **kwargs,  # type: ignore[arg-type]
    )


_BEDROCK = InferenceTarget(provider="bedrock", model="anthropic.claude-x", region="eu-west-1")


def test_compila_cuando_el_modelo_admite_la_herramienta() -> None:
    """A tool the binding admits becomes a compiled grant."""
    compiled, issues = _compile(
        _spec("web_search"),
        inference=_BEDROCK,
        native_tools=lambda _t: frozenset({"web_search"}),
    )

    assert issues == []
    assert compiled == (CompiledNativeCapability(tool="web_search"),)


def test_falla_nombrando_proveedor_modelo_rol_y_admitidas_cuando_no_la_admite() -> None:
    """The message says what was asked for and what the binding does admit."""
    _compiled, issues = _compile(
        _spec("web_search"),
        inference=_BEDROCK,
        native_tools=lambda _t: frozenset({"code_execution"}),
    )

    assert len(issues) == 1
    issue = issues[0]
    assert issue.code is AgentErrorCode.NATIVE_TOOL_UNSUPPORTED  # type: ignore[attr-defined]
    assert issue.field == "capabilities.tool"  # type: ignore[attr-defined]
    for expected in ("bedrock", "anthropic.claude-x", "default", "web_search", "code_execution"):
        assert expected in issue.message  # type: ignore[attr-defined]


def test_falla_una_sola_vez_cuando_la_misma_herramienta_se_concede_dos_veces() -> None:
    """A tool granted twice is one issue, not two grants."""
    _compiled, issues = _compile(
        _spec("web_search", "web_search"),
        inference=_BEDROCK,
        native_tools=lambda _t: frozenset({"web_search"}),
    )

    assert [issue.code for issue in issues] == [AgentErrorCode.NATIVE_TOOL_DUPLICATE]  # type: ignore[attr-defined]


def test_no_añade_incidencia_cuando_el_rol_no_esta_ligado() -> None:
    """An unbound role is reported by role resolution, not twice."""
    compiled, issues = _compile(_spec("web_search"), inference=None, native_tools=None)

    assert issues == []
    assert compiled == ()


def test_propaga_la_incidencia_del_oraculo_cuando_el_sdk_falta() -> None:
    """A provider SDK missing is the oracle's issue, not a traceback."""

    def _missing(_target: InferenceTarget) -> frozenset[str]:
        raise AgentCompilationError([provider_not_installed("openai", "ai-openai")])

    _compiled, issues = _compile(_spec("web_search"), inference=_BEDROCK, native_tools=_missing)

    assert [issue.code for issue in issues] == [AgentErrorCode.PROVIDER_NOT_INSTALLED]  # type: ignore[attr-defined]


def test_rechaza_el_kind_cuando_el_motor_no_aporta_oraculo() -> None:
    """Without an oracle the grant cannot be checked, so it is refused."""
    _compiled, issues = _compile(_spec("web_search"), inference=_BEDROCK, native_tools=None)

    assert [issue.code for issue in issues] == [AgentErrorCode.CAPABILITY_KIND_UNSUPPORTED]  # type: ignore[attr-defined]


@pytest.mark.parametrize("tool", ["web_search", "web_fetch", "code_execution"])
def test_acepta_cada_herramienta_del_vocabulario(tool: str) -> None:
    """Every name the artifact schema admits compiles."""
    compiled, issues = _compile(
        _spec(tool), inference=_BEDROCK, native_tools=lambda _t: frozenset({tool})
    )

    assert issues == []
    assert compiled == (CompiledNativeCapability(tool=tool),)
