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
    INVOKER_MISSING_REASON,
    AgentErrorCode,
    a2a_agent_unreachable,
    agent_marker_output_mismatch,
    agent_marker_unknown,
    conversation_input_unsatisfied,
    conversation_invoker_missing,
    conversation_usecase_also_granted,
    conversation_usecase_unknown,
    mcp_marker_unknown,
    mcp_server_unreachable,
    mcp_transport_invalid,
    on_output_input_unsatisfied,
    on_output_invoker_missing,
    on_output_usecase_also_granted,
    on_output_usecase_unknown,
    provider_unknown,
    use_case_tool_filter_matches_nothing,
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


def test_on_output_invoker_missing_conserva_el_texto_original_cuando_no_hay_razon() -> None:
    """The default message is byte-identical to the one shipped before the shared constant."""
    issue = on_output_invoker_missing(["a"])

    assert INVOKER_MISSING_REASON in issue.message
    assert issue.message == "agents declare an output hook but no use-case invoker is configured: a"


def test_conversation_usecase_unknown_apunta_al_campo_conversation_usecase() -> None:
    """The unknown key is attributed to the loader field, not to the capabilities."""
    issue = conversation_usecase_unknown("triage-bot", "conversations.load")

    assert issue.code is AgentErrorCode.CONVERSATION_USECASE_UNKNOWN
    assert issue.component == "triage-bot"
    assert issue.field == "conversation.usecase"
    assert "conversations.load" in issue.message


def test_conversation_input_unsatisfied_lleva_la_razon_en_el_mensaje() -> None:
    """The reason is the only clue the author gets about which Input field fails."""
    issue = conversation_input_unsatisfied(
        "triage-bot", "conversations.load", "field 'tenant' has no default"
    )

    assert issue.code is AgentErrorCode.CONVERSATION_INPUT_UNSATISFIED
    assert issue.component == "triage-bot"
    assert issue.field == "conversation.usecase"
    assert "conversations.load" in issue.message
    assert "field 'tenant' has no default" in issue.message


def test_conversation_usecase_also_granted_apunta_al_campo_conversation_usecase() -> None:
    """A key that is both loader and capability is reported once, on the loader field."""
    issue = conversation_usecase_also_granted("triage-bot", "conversations.load")

    assert issue.code is AgentErrorCode.CONVERSATION_USECASE_ALSO_GRANTED
    assert issue.component == "triage-bot"
    assert issue.field == "conversation.usecase"
    assert "conversations.load" in issue.message


def test_conversation_invoker_missing_es_un_problema_de_despliegue_que_nombra_los_agentes() -> None:
    """No single agent owns the missing invoker, so the issue belongs to ``ai``."""
    issue = conversation_invoker_missing(["triage-bot", "escalation-bot"])

    assert issue.code is AgentErrorCode.CONVERSATION_INVOKER_MISSING
    assert issue.component == "ai"
    assert issue.field == "conversation"
    assert "triage-bot, escalation-bot" in issue.message


def test_conversation_invoker_missing_usa_la_razon_por_defecto_cuando_no_se_indica() -> None:
    """Both invoker-missing issues share one default reason, so operators read one wording."""
    issue = conversation_invoker_missing(["a"])

    assert INVOKER_MISSING_REASON in issue.message
    assert (
        issue.message
        == "agents declare a conversation loader but no use-case invoker is configured: a"
    )


def test_conversation_invoker_missing_lleva_la_razon_cuando_se_indica() -> None:
    """An invoker that exists but is unusable is reported with its own reason."""
    issue = conversation_invoker_missing(["triage-bot"], reason="the use-case invoker is unbound")

    assert issue.code is AgentErrorCode.CONVERSATION_INVOKER_MISSING
    assert "the use-case invoker is unbound" in issue.message
    assert "triage-bot" in issue.message


def test_mcp_transport_invalid_nombra_el_componente_y_lleva_la_razon() -> None:
    """The reason is the only clue the operator gets about which transport rule fails."""
    issue = mcp_transport_invalid("ai.mcp_servers.search", "transport 'ws' is not supported")

    assert issue.code is AgentErrorCode.MCP_TRANSPORT_INVALID
    assert issue.component == "ai.mcp_servers.search"
    assert issue.field == "transport"
    assert "ai.mcp_servers.search" in issue.message
    assert "transport 'ws' is not supported" in issue.message


def test_agent_marker_unknown_apunta_al_parametro_del_use_case() -> None:
    """The offending field is the parameter, not the use case or the agent."""
    issue = agent_marker_unknown(
        "incidents.report", "triage", "incident-triage", ["escalation-bot"]
    )

    assert issue.code is AgentErrorCode.AGENT_MARKER_UNKNOWN
    assert issue.component == "incidents.report"
    assert issue.field == "parameters.triage"
    assert "incident-triage" in issue.message
    assert "escalation-bot" in issue.message


def test_agent_marker_unknown_declara_ninguno_cuando_no_hay_agentes_compilados() -> None:
    """An empty deployment still produces a readable message."""
    issue = agent_marker_unknown("incidents.report", "triage", "incident-triage", [])

    assert "none" in issue.message


def test_mcp_marker_unknown_apunta_al_parametro_del_use_case() -> None:
    """The offending field is the parameter, not the use case or the server.

    ``parameter`` and ``server`` are deliberately disjoint strings (neither is
    a substring of the other): a fixture where the parameter name is a
    substring of the server name (e.g. ``"docs"`` inside ``"docs-server"``)
    would let ``assert parameter in message`` pass on the server's presence
    alone, without the message actually naming the parameter.
    """
    issue = mcp_marker_unknown("incidents.report", "gateway", "docs-server", ["billing-server"])

    assert issue.code is AgentErrorCode.MCP_MARKER_UNKNOWN
    assert issue.component == "incidents.report"
    assert issue.field == "parameters.gateway"
    assert "incidents.report" in issue.message
    assert "gateway" in issue.message
    assert "docs-server" in issue.message
    assert "billing-server" in issue.message


def test_mcp_marker_unknown_declara_ninguno_cuando_no_hay_servidores_configurados() -> None:
    """An empty deployment still produces a readable message."""
    issue = mcp_marker_unknown("incidents.report", "docs", "docs-server", [])

    assert "none" in issue.message


def test_use_case_tool_filter_matches_nothing_reutiliza_el_codigo_existente() -> None:
    """One condition, one code: no new code is minted for this message.

    ``parameter`` and ``server`` are deliberately disjoint strings (see the
    equivalent note on ``test_mcp_marker_unknown_apunta_al_parametro_del_use_case``),
    so each assertion below can only pass if the message actually names that
    value, not because it is a substring of another fixture value.
    """
    issue = use_case_tool_filter_matches_nothing("incidents.report", "gateway", "docs-server")

    assert issue.code is AgentErrorCode.TOOL_FILTER_MATCHES_NOTHING
    assert issue.component == "incidents.report"
    assert issue.field == "parameters.gateway"
    assert "incidents.report" in issue.message
    assert "gateway" in issue.message
    assert "docs-server" in issue.message


def test_agent_marker_output_mismatch_lleva_lo_esperado_y_lo_declarado() -> None:
    """Both the annotation's type and the agent's own declared type appear."""
    issue = agent_marker_output_mismatch(
        "incidents.report",
        "triage",
        "incident-triage",
        "SeverityAssessment",
        "TriageVerdict",
    )

    assert issue.code is AgentErrorCode.AGENT_MARKER_OUTPUT_MISMATCH
    assert issue.component == "incidents.report"
    assert issue.field == "parameters.triage"
    assert "SeverityAssessment" in issue.message
    assert "TriageVerdict" in issue.message
    assert "incident-triage" in issue.message
