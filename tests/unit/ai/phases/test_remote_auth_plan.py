"""The artifact names an endpoint; the deployment says how it authenticates.

This module pins the property that makes the whole design worth having: the
same artifact compiles unchanged whether the MCP server or the remote A2A agent
it names needs no credential, a fixed header, or a named strategy. Only the
plan differs, because only the deployment differs.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from loom.ai.compiler import (
    AgentPlan,
    CompiledA2ACapability,
    CompiledMcpCapability,
    CompiledRemoteAuth,
)
from loom.ai.config import A2AAgentConfig, AiConfig, McpServerConfig
from loom.ai.declarative import A2ACapability, AgentSpecV1, McpCapability

_SERVER = "knowledge"
_URL = "https://knowledge.example.com/mcp"
_AGENT = "market"
_AGENT_URL = "https://market.example.com/a2a"


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

        assert _mcp_capability(plan).auth == CompiledRemoteAuth(
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


@pytest.fixture
def a2a_artifact(spec_factory: Callable[..., AgentSpecV1]) -> AgentSpecV1:
    """One artifact, reused across every deployment: it names the remote agent only."""
    return spec_factory(capabilities=(A2ACapability(agent=_AGENT),))


@pytest.fixture
def config_naming_agent(ai_config_factory: Callable[..., AiConfig]) -> Callable[..., AiConfig]:
    """Build a deployment whose single remote agent is configured as asked."""

    def _make(**agent_settings: Any) -> AiConfig:
        return ai_config_factory(
            a2a_agents={_AGENT: A2AAgentConfig(url=_AGENT_URL, **agent_settings)}
        )

    return _make


def _a2a_capability(plan: AgentPlan) -> CompiledA2ACapability:
    """Return the single A2A grant of a compiled plan."""
    capability = next(c for c in plan.capabilities if c.kind == "a2a")
    assert isinstance(capability, CompiledA2ACapability)
    return capability


class TestElArtefactoA2ANoCambiaEntreEntornos:
    """Criterion 8: one artifact, three deployments, three clean compilations."""

    @pytest.mark.parametrize(
        "agent_settings",
        [
            {},
            {"headers_ref": "X-API-Key=abc123"},
            {"auth": {"kind": "bearer", "token_ref": "a.b-c_1"}},
        ],
        ids=["sin_credencial", "headers_ref", "estrategia"],
    )
    def test_compila_el_mismo_artefacto_sea_cual_sea_la_credencial(
        self,
        a2a_artifact: AgentSpecV1,
        plan_for: Callable[..., AgentPlan],
        config_naming_agent: Callable[..., AiConfig],
        agent_settings: dict[str, Any],
    ) -> None:
        plan = plan_for(a2a_artifact, config=config_naming_agent(**agent_settings))

        assert _a2a_capability(plan).agent == _AGENT


class TestElPlanA2ALlevaLaCredencialResuelta:
    """The strategy name dies at compile: the engine never re-reads configuration."""

    def test_no_lleva_auth_cuando_el_agente_no_declara_ninguna(
        self,
        a2a_artifact: AgentSpecV1,
        plan_for: Callable[..., AgentPlan],
        config_naming_agent: Callable[..., AiConfig],
    ) -> None:
        plan = plan_for(a2a_artifact, config=config_naming_agent())

        assert _a2a_capability(plan).auth is None

    def test_separa_kind_de_sus_ajustes_cuando_el_agente_declara_estrategia(
        self,
        a2a_artifact: AgentSpecV1,
        plan_for: Callable[..., AgentPlan],
        config_naming_agent: Callable[..., AiConfig],
    ) -> None:
        config = config_naming_agent(auth={"kind": "bearer", "token_ref": "a.b-c_1"})

        plan = plan_for(a2a_artifact, config=config)

        assert _a2a_capability(plan).auth == CompiledRemoteAuth(
            kind="bearer", settings=(("token_ref", "a.b-c_1"),)
        )

    def test_lleva_headers_ref_cuando_el_agente_lo_declara(
        self,
        a2a_artifact: AgentSpecV1,
        plan_for: Callable[..., AgentPlan],
        config_naming_agent: Callable[..., AiConfig],
    ) -> None:
        plan = plan_for(a2a_artifact, config=config_naming_agent(headers_ref="X-API-Key=abc123"))

        assert _a2a_capability(plan).headers_ref == "X-API-Key=abc123"
