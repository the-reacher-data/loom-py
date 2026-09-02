"""The artifact names a server; the deployment says how it authenticates.

This module pins the property that makes the whole design worth having: the
same artifact compiles unchanged whether the server it names needs no
credential, a fixed header, or a named strategy. Only the plan differs, because
only the deployment differs.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from loom.ai.compiler import AgentPlan, CompiledMcpAuth, CompiledMcpCapability
from loom.ai.config import AiConfig, McpServerConfig
from loom.ai.declarative import AgentSpecV1, McpCapability

_SERVER = "knowledge"
_URL = "https://knowledge.example.com/mcp"


def _mcp_capability(plan: AgentPlan) -> CompiledMcpCapability:
    """Return the single MCP grant of a compiled plan."""
    capability = next(c for c in plan.capabilities if c.kind == "mcp")
    assert isinstance(capability, CompiledMcpCapability)
    return capability


@pytest.fixture
def artifact(spec_factory: Callable[..., AgentSpecV1]) -> AgentSpecV1:
    """One artifact, reused across every deployment: it names the server only."""
    return spec_factory(capabilities=(McpCapability(server=_SERVER),))


@pytest.fixture
def config_naming(ai_config_factory: Callable[..., AiConfig]) -> Callable[..., AiConfig]:
    """Build a deployment whose single MCP server is configured as asked."""

    def _make(**server_settings: Any) -> AiConfig:
        return ai_config_factory(
            mcp_servers={_SERVER: McpServerConfig(url=_URL, **server_settings)}
        )

    return _make


class TestElArtefactoNoCambiaEntreEntornos:
    """Criterion: one artifact, three deployments, three clean compilations."""

    @pytest.mark.parametrize(
        "server_settings",
        [
            {},
            {"headers_ref": "X-API-Key=abc123"},
            {"auth": {"kind": "oauth"}},
        ],
        ids=["sin_credencial", "headers_ref", "estrategia"],
    )
    def test_compila_el_mismo_artefacto_sea_cual_sea_la_credencial(
        self,
        artifact: AgentSpecV1,
        plan_for: Callable[..., AgentPlan],
        config_naming: Callable[..., AiConfig],
        server_settings: dict[str, Any],
    ) -> None:
        plan = plan_for(artifact, config=config_naming(**server_settings))

        assert _mcp_capability(plan).server == _SERVER


class TestElPlanLlevaLaCredencialResuelta:
    """The name dies at compile: the plan carries the strategy, not the lookup."""

    def test_no_lleva_auth_cuando_el_servidor_no_declara_ninguna(
        self,
        artifact: AgentSpecV1,
        plan_for: Callable[..., AgentPlan],
        config_naming: Callable[..., AiConfig],
    ) -> None:
        plan = plan_for(artifact, config=config_naming())

        assert _mcp_capability(plan).auth is None

    def test_separa_kind_de_sus_ajustes_cuando_el_servidor_declara_estrategia(
        self,
        artifact: AgentSpecV1,
        plan_for: Callable[..., AgentPlan],
        config_naming: Callable[..., AiConfig],
    ) -> None:
        """The engine must never have to dig ``kind`` out of the settings again."""
        config = config_naming(
            auth={"kind": "static", "headers_ref": "X-API-Key=abc123"},
        )

        plan = plan_for(artifact, config=config)

        assert _mcp_capability(plan).auth == CompiledMcpAuth(
            kind="static", settings=(("headers_ref", "X-API-Key=abc123"),)
        )

    def test_lleva_headers_ref_cuando_el_servidor_lo_declara(
        self,
        artifact: AgentSpecV1,
        plan_for: Callable[..., AgentPlan],
        config_naming: Callable[..., AiConfig],
    ) -> None:
        plan = plan_for(artifact, config=config_naming(headers_ref="X-API-Key=abc123"))

        assert _mcp_capability(plan).headers_ref == "X-API-Key=abc123"
