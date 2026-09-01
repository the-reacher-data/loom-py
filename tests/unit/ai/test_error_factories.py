"""Naming contracts of the compilation-issue factories in ``loom.ai.errors``.

Two facts these factories must keep, because both are load-bearing for the
redaction guarantee (FR-030a/FR-038) and for the "every problem names the
offending field" principle:

* ``*_unreachable`` receives the *registered name* of a server or a remote
  agent, never its URL. The parameter name is the only instruction the next
  caller reads, so it is pinned here.
* An unknown provider is not an uninstalled one: there is no extra to install,
  and the message must not tell an operator to install one.
"""

from __future__ import annotations

import inspect

from loom.ai.errors import (
    AgentErrorCode,
    a2a_agent_unreachable,
    mcp_server_unreachable,
    provider_unknown,
)


def _first_parameter(factory: object) -> str:
    return next(iter(inspect.signature(factory).parameters))  # type: ignore[arg-type]


def test_mcp_server_unreachable_nombra_su_parametro_server_no_url() -> None:
    """A parameter called ``url`` invites the next caller to pass a URL."""
    assert _first_parameter(mcp_server_unreachable) == "server"


def test_a2a_agent_unreachable_nombra_su_parametro_agent_no_url() -> None:
    assert _first_parameter(a2a_agent_unreachable) == "agent"


def test_mcp_server_unreachable_lleva_el_nombre_registrado_al_componente() -> None:
    issue = mcp_server_unreachable("reporting-mcp", "connection refused")

    assert issue.code is AgentErrorCode.MCP_SERVER_UNREACHABLE
    assert issue.component == "reporting-mcp"
    assert "reporting-mcp" in issue.message


def test_a2a_agent_unreachable_lleva_el_nombre_registrado_al_componente() -> None:
    issue = a2a_agent_unreachable("pricing-desk", "card not retrievable")

    assert issue.code is AgentErrorCode.A2A_AGENT_UNREACHABLE
    assert issue.component == "pricing-desk"
    assert "pricing-desk" in issue.message


def test_provider_unknown_enumera_los_soportados_sin_mandar_instalar_nada() -> None:
    """``PROVIDER_UNKNOWN`` is not ``PROVIDER_NOT_INSTALLED``: no extra exists."""
    issue = provider_unknown("unheard-of", ["anthropic", "openai"])

    assert issue.code is AgentErrorCode.PROVIDER_UNKNOWN
    assert issue.component == "unheard-of"
    assert "anthropic, openai" in issue.message
    assert "extra" not in issue.message
    assert "install" not in issue.message
