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
    on_output_input_unsatisfied,
    on_output_invoker_missing,
    on_output_usecase_also_granted,
    on_output_usecase_unknown,
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


def test_on_output_usecase_unknown_apunta_al_campo_on_output_usecase() -> None:
    """The unknown key is attributed to the hook field, not to the capabilities."""
    issue = on_output_usecase_unknown("triage-bot", "incidents.record_triage")

    assert issue.code is AgentErrorCode.ON_OUTPUT_USECASE_UNKNOWN
    assert issue.component == "triage-bot"
    assert issue.field == "on_output.usecase"
    assert "incidents.record_triage" in issue.message


def test_on_output_input_unsatisfied_lleva_la_razon_en_el_mensaje() -> None:
    """The reason is the only clue the author gets about which Input field fails."""
    issue = on_output_input_unsatisfied(
        "triage-bot", "incidents.record_triage", "field 'reviewer_email' has no default"
    )

    assert issue.code is AgentErrorCode.ON_OUTPUT_INPUT_UNSATISFIED
    assert issue.component == "triage-bot"
    assert issue.field == "on_output.usecase"
    assert "incidents.record_triage" in issue.message
    assert "field 'reviewer_email' has no default" in issue.message


def test_on_output_usecase_also_granted_apunta_al_campo_on_output_usecase() -> None:
    """A key that is both hook and capability is reported once, on the hook field."""
    issue = on_output_usecase_also_granted("triage-bot", "incidents.record_triage")

    assert issue.code is AgentErrorCode.ON_OUTPUT_USECASE_ALSO_GRANTED
    assert issue.component == "triage-bot"
    assert issue.field == "on_output.usecase"
    assert "incidents.record_triage" in issue.message


def test_on_output_invoker_missing_es_un_problema_de_despliegue_que_nombra_los_agentes() -> None:
    """No single agent owns the missing invoker, so the issue belongs to ``ai``."""
    issue = on_output_invoker_missing(["triage-bot", "escalation-bot"])

    assert issue.code is AgentErrorCode.ON_OUTPUT_INVOKER_MISSING
    assert issue.component == "ai"
    assert issue.field == "on_output"
    assert "triage-bot, escalation-bot" in issue.message
