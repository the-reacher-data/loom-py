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
    instruction_block_invalid,
    mcp_marker_unknown,
    mcp_server_unreachable,
    mcp_transport_invalid,
    on_output_input_unsatisfied,
    on_output_invoker_missing,
    on_output_usecase_also_granted,
    on_output_usecase_unknown,
    output_schema_invalid,
    provider_unknown,
    state_declaration_conflict,
    state_schema_invalid,
    state_surface_unsupported,
    state_type_ref_unresolvable,
    state_type_ref_unsupported,
    template_compilation_failed,
    template_extra_missing,
    use_case_tool_filter_matches_nothing,
)


def _first_parameter(factory: object) -> str:
    return next(iter(inspect.signature(factory).parameters))  # type: ignore[arg-type]


def test_mcp_server_unreachable_names_its_parameter_server_not_url() -> None:
    """A parameter called ``url`` invites the next caller to pass a URL."""
    assert _first_parameter(mcp_server_unreachable) == "server"


def test_a2a_agent_unreachable_names_its_parameter_agent_not_url() -> None:
    assert _first_parameter(a2a_agent_unreachable) == "agent"


def test_mcp_server_unreachable_carries_the_registered_name_to_the_component() -> None:
    issue = mcp_server_unreachable("reporting-mcp", "connection refused")

    assert issue.code is AgentErrorCode.MCP_SERVER_UNREACHABLE
    assert issue.component == "reporting-mcp"
    assert "reporting-mcp" in issue.message


def test_a2a_agent_unreachable_carries_the_registered_name_to_the_component() -> None:
    issue = a2a_agent_unreachable("pricing-desk", "card not retrievable")

    assert issue.code is AgentErrorCode.A2A_AGENT_UNREACHABLE
    assert issue.component == "pricing-desk"
    assert "pricing-desk" in issue.message


def test_provider_unknown_lists_the_supported_providers_without_telling_to_install() -> None:
    """``PROVIDER_UNKNOWN`` is not ``PROVIDER_NOT_INSTALLED``: no extra exists."""
    issue = provider_unknown("unheard-of", ["anthropic", "openai"])

    assert issue.code is AgentErrorCode.PROVIDER_UNKNOWN
    assert issue.component == "unheard-of"
    assert "anthropic, openai" in issue.message
    assert "extra" not in issue.message
    assert "install" not in issue.message


def test_on_output_usecase_unknown_points_at_the_on_output_usecase_field() -> None:
    """The unknown key is attributed to the hook field, not to the capabilities."""
    issue = on_output_usecase_unknown("triage-bot", "incidents.record_triage")

    assert issue.code is AgentErrorCode.ON_OUTPUT_USECASE_UNKNOWN
    assert issue.component == "triage-bot"
    assert issue.field == "on_output.usecase"
    assert "incidents.record_triage" in issue.message


def test_on_output_input_unsatisfied_carries_the_reason_in_the_message() -> None:
    """The reason is the only clue the author gets about which Input field fails."""
    issue = on_output_input_unsatisfied(
        "triage-bot", "incidents.record_triage", "field 'reviewer_email' has no default"
    )

    assert issue.code is AgentErrorCode.ON_OUTPUT_INPUT_UNSATISFIED
    assert issue.component == "triage-bot"
    assert issue.field == "on_output.usecase"
    assert "incidents.record_triage" in issue.message
    assert "field 'reviewer_email' has no default" in issue.message


def test_on_output_usecase_also_granted_points_at_the_on_output_usecase_field() -> None:
    """A key that is both hook and capability is reported once, on the hook field."""
    issue = on_output_usecase_also_granted("triage-bot", "incidents.record_triage")

    assert issue.code is AgentErrorCode.ON_OUTPUT_USECASE_ALSO_GRANTED
    assert issue.component == "triage-bot"
    assert issue.field == "on_output.usecase"
    assert "incidents.record_triage" in issue.message


def test_on_output_invoker_missing_is_a_deployment_issue_naming_the_agents() -> None:
    """No single agent owns the missing invoker, so the issue belongs to ``ai``."""
    issue = on_output_invoker_missing(["triage-bot", "escalation-bot"])

    assert issue.code is AgentErrorCode.ON_OUTPUT_INVOKER_MISSING
    assert issue.component == "ai"
    assert issue.field == "on_output"
    assert "triage-bot, escalation-bot" in issue.message


def test_on_output_invoker_missing_keeps_the_original_text_without_a_reason() -> None:
    """The default message is byte-identical to the one shipped before the shared constant."""
    issue = on_output_invoker_missing(["a"])

    assert INVOKER_MISSING_REASON in issue.message
    assert issue.message == "agents declare an output hook but no use-case invoker is configured: a"


def test_conversation_usecase_unknown_points_at_the_conversation_usecase_field() -> None:
    """The unknown key is attributed to the loader field, not to the capabilities."""
    issue = conversation_usecase_unknown("triage-bot", "conversations.load")

    assert issue.code is AgentErrorCode.CONVERSATION_USECASE_UNKNOWN
    assert issue.component == "triage-bot"
    assert issue.field == "conversation.usecase"
    assert "conversations.load" in issue.message


def test_conversation_input_unsatisfied_carries_the_reason_in_the_message() -> None:
    """The reason is the only clue the author gets about which Input field fails."""
    issue = conversation_input_unsatisfied(
        "triage-bot", "conversations.load", "field 'tenant' has no default"
    )

    assert issue.code is AgentErrorCode.CONVERSATION_INPUT_UNSATISFIED
    assert issue.component == "triage-bot"
    assert issue.field == "conversation.usecase"
    assert "conversations.load" in issue.message
    assert "field 'tenant' has no default" in issue.message


def test_conversation_usecase_also_granted_points_at_the_conversation_usecase_field() -> None:
    """A key that is both loader and capability is reported once, on the loader field."""
    issue = conversation_usecase_also_granted("triage-bot", "conversations.load")

    assert issue.code is AgentErrorCode.CONVERSATION_USECASE_ALSO_GRANTED
    assert issue.component == "triage-bot"
    assert issue.field == "conversation.usecase"
    assert "conversations.load" in issue.message


def test_conversation_invoker_missing_is_a_deployment_issue_naming_the_agents() -> None:
    """No single agent owns the missing invoker, so the issue belongs to ``ai``."""
    issue = conversation_invoker_missing(["triage-bot", "escalation-bot"])

    assert issue.code is AgentErrorCode.CONVERSATION_INVOKER_MISSING
    assert issue.component == "ai"
    assert issue.field == "conversation"
    assert "triage-bot, escalation-bot" in issue.message


def test_conversation_invoker_missing_uses_the_default_reason_when_none_is_given() -> None:
    """Both invoker-missing issues share one default reason, so operators read one wording."""
    issue = conversation_invoker_missing(["a"])

    assert INVOKER_MISSING_REASON in issue.message
    assert (
        issue.message
        == "agents declare a conversation loader but no use-case invoker is configured: a"
    )


def test_conversation_invoker_missing_carries_the_reason_when_given() -> None:
    """An invoker that exists but is unusable is reported with its own reason."""
    issue = conversation_invoker_missing(["triage-bot"], reason="the use-case invoker is unbound")

    assert issue.code is AgentErrorCode.CONVERSATION_INVOKER_MISSING
    assert "the use-case invoker is unbound" in issue.message
    assert "triage-bot" in issue.message


def test_mcp_transport_invalid_names_the_component_and_carries_the_reason() -> None:
    """The reason is the only clue the operator gets about which transport rule fails."""
    issue = mcp_transport_invalid("ai.mcp_servers.search", "transport 'ws' is not supported")

    assert issue.code is AgentErrorCode.MCP_TRANSPORT_INVALID
    assert issue.component == "ai.mcp_servers.search"
    assert issue.field == "transport"
    assert "ai.mcp_servers.search" in issue.message
    assert "transport 'ws' is not supported" in issue.message


def test_agent_marker_unknown_points_at_the_use_cases_parameter() -> None:
    """The offending field is the parameter, not the use case or the agent."""
    issue = agent_marker_unknown(
        "incidents.report", "triage", "incident-triage", ["escalation-bot"]
    )

    assert issue.code is AgentErrorCode.AGENT_MARKER_UNKNOWN
    assert issue.component == "incidents.report"
    assert issue.field == "parameters.triage"
    assert "incident-triage" in issue.message
    assert "escalation-bot" in issue.message


def test_agent_marker_unknown_declares_none_when_no_agents_are_compiled() -> None:
    """An empty deployment still produces a readable message."""
    issue = agent_marker_unknown("incidents.report", "triage", "incident-triage", [])

    assert "none" in issue.message


def test_mcp_marker_unknown_points_at_the_use_cases_parameter() -> None:
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


def test_mcp_marker_unknown_declares_none_when_no_servers_are_configured() -> None:
    """An empty deployment still produces a readable message."""
    issue = mcp_marker_unknown("incidents.report", "docs", "docs-server", [])

    assert "none" in issue.message


def test_use_case_tool_filter_matches_nothing_reuses_the_existing_code() -> None:
    """One condition, one code: no new code is minted for this message.

    ``parameter`` and ``server`` are deliberately disjoint strings (see the
    equivalent note on ``test_mcp_marker_unknown_points_at_the_use_cases_parameter``),
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


def test_agent_marker_output_mismatch_carries_expected_and_declared_types() -> None:
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


def test_state_declaration_conflict_points_at_deps_type() -> None:
    """Declaring both ``deps_type`` and ``deps_schema`` names the artifact."""
    issue = state_declaration_conflict("appraisal-bot")

    assert issue.code is AgentErrorCode.STATE_DECLARATION_CONFLICT
    assert issue.component == "appraisal-bot"
    assert issue.field == "deps_type"
    assert "deps_type" in issue.message
    assert "deps_schema" in issue.message
    assert "appraisal-bot" in issue.message


def test_state_type_ref_unresolvable_names_the_reference() -> None:
    """A ``deps_type`` symbol that cannot be imported names the reference."""
    issue = state_type_ref_unresolvable("appraisal-bot", "myapp.agents:AppraisalDeps")

    assert issue.code is AgentErrorCode.STATE_TYPE_REF_UNRESOLVABLE
    assert issue.component == "appraisal-bot"
    assert issue.field == "deps_type"
    assert "myapp.agents:AppraisalDeps" in issue.message


def test_state_type_ref_unsupported_carries_the_reason() -> None:
    """A resolved symbol msgspec refuses reports the checker's own reason."""
    issue = state_type_ref_unsupported(
        "appraisal-bot", "myapp.agents:AppraisalDeps", "not a msgspec Struct"
    )

    assert issue.code is AgentErrorCode.STATE_TYPE_REF_UNSUPPORTED
    assert issue.component == "appraisal-bot"
    assert issue.field == "deps_type"
    assert "myapp.agents:AppraisalDeps" in issue.message
    assert "not a msgspec Struct" in issue.message


def test_state_type_ref_unresolvable_and_unsupported_are_distinct_codes() -> None:
    """A missing symbol and an unschematisable one are not collapsed."""
    unresolvable = state_type_ref_unresolvable("appraisal-bot", "myapp.agents:Missing")
    unsupported = state_type_ref_unsupported("appraisal-bot", "myapp.agents:Present", "reason")

    assert unresolvable.code is not unsupported.code


def test_state_schema_invalid_names_the_reason() -> None:
    """An invalid ``deps_schema`` object reports why it is not a JSON Schema."""
    issue = state_schema_invalid("appraisal-bot", "'type' is not a recognised keyword")

    assert issue.code is AgentErrorCode.STATE_SCHEMA_INVALID
    assert issue.component == "appraisal-bot"
    assert issue.field == "deps_schema"
    assert "'type' is not a recognised keyword" in issue.message


def test_state_schema_invalid_does_not_reuse_output_schema_invalid() -> None:
    """A state-schema fault and an output-schema fault must be filterable apart."""
    state_issue = state_schema_invalid("appraisal-bot", "reason")
    output_issue = output_schema_invalid("appraisal-bot", "reason")

    assert state_issue.code is not output_issue.code
    assert state_issue.field != output_issue.field


def test_state_surface_unsupported_names_the_artifact_and_the_surface() -> None:
    """Both halves of the conflict — declared state and the surface — are named."""
    issue = state_surface_unsupported("appraisal-bot", "a2a")

    assert issue.code is AgentErrorCode.STATE_SURFACE_UNSUPPORTED
    assert issue.component == "appraisal-bot"
    assert issue.field == "deps_type"
    assert "appraisal-bot" in issue.message
    assert "a2a" in issue.message


def test_instruction_block_invalid_names_the_reason() -> None:
    """A compile-time instruction rule violation carries its own reason."""
    issue = instruction_block_invalid(
        "appraisal-bot", "duplicate block name 'base' at positions 0 and 2"
    )

    assert issue.code is AgentErrorCode.INSTRUCTION_BLOCK_INVALID
    assert issue.component == "appraisal-bot"
    assert issue.field == "instructions"
    assert "duplicate block name 'base' at positions 0 and 2" in issue.message


def test_template_compilation_failed_names_the_block_and_the_checkers_reason() -> None:
    """The block is named by identity, and the checker's own message is preserved."""
    issue = template_compilation_failed(
        "appraisal-bot", "context", "unknown marker 'motor.ccc' at $.motor"
    )

    assert issue.code is AgentErrorCode.TEMPLATE_COMPILATION_FAILED
    assert issue.component == "appraisal-bot"
    assert issue.field == "instructions"
    assert "context" in issue.message
    assert "unknown marker 'motor.ccc' at $.motor" in issue.message


def test_template_extra_missing_names_the_block_and_the_extra() -> None:
    """The message names both the block that declares ``template:`` and the extra."""
    issue = template_extra_missing("appraisal-bot", "context", "handlebars")

    assert issue.code is AgentErrorCode.TEMPLATE_EXTRA_MISSING
    assert issue.component == "appraisal-bot"
    assert issue.field == "instructions"
    assert "context" in issue.message
    assert "handlebars" in issue.message


def test_state_and_instruction_messages_never_embed_a_url() -> None:
    """None of the eight new factories embeds a URL.

    ``state_type_ref_unsupported``, ``state_schema_invalid``,
    ``instruction_block_invalid`` and ``template_compilation_failed``
    interpolate a caller-supplied ``reason`` verbatim, and
    ``template_compilation_failed``'s ``reason`` is documented as the template
    checker's own message, which can carry a fragment of the source template.
    Redacting that is the caller's responsibility, not this test's.
    """
    issues = [
        state_declaration_conflict("appraisal-bot"),
        state_type_ref_unresolvable("appraisal-bot", "myapp.agents:AppraisalDeps"),
        state_type_ref_unsupported("appraisal-bot", "myapp.agents:AppraisalDeps", "reason"),
        state_schema_invalid("appraisal-bot", "reason"),
        state_surface_unsupported("appraisal-bot", "a2a"),
        instruction_block_invalid("appraisal-bot", "reason"),
        template_compilation_failed("appraisal-bot", "context", "reason"),
        template_extra_missing("appraisal-bot", "context", "handlebars"),
    ]

    for issue in issues:
        assert "http://" not in issue.message
        assert "https://" not in issue.message
