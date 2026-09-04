"""Agent compilation and run-time error codes.

Two disjoint catalogues, mirroring :mod:`loom.streaming.compiler._errors`:

``AgentErrorCode``
    Compile-time failures.  A broken artifact or an unresolvable deployment
    produces one or more :class:`AgentCompilationIssue`, and
    :class:`AgentCompilationError` aggregates them so a single run reports
    every problem at once.

``AgentRunErrorCode``
    Execution outcomes.  Each code belongs to an :class:`AgentRunErrorClass`
    and the retry policy reads the *class*, never the message (FR-028).

Every code has a dedicated factory function so call-sites stay
intention-revealing and free of string formatting.

Issue factories
    The factories below share one contract, stated here once rather than
    repeated on each of them.  Every factory returns a single
    :class:`AgentCompilationIssue` carrying its own :class:`AgentErrorCode`.
    ``component`` names the artifact component or configuration path the
    issue is attributed to — ``"market"``, ``"ai.mcp_servers.data"``.  Every
    other parameter is interpolated into the human-readable ``message`` and
    is never read for control flow: callers branch on the code, never on the
    text.  Messages never carry secret material — in credential-related
    issues the offending value is deliberately omitted.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from enum import StrEnum
from types import MappingProxyType
from typing import Final

from loom.core.model import LoomFrozenStruct


class AgentErrorCode(StrEnum):
    """Enumeration of all agent compile-time failure codes."""

    # Envelope and structure
    SPEC_VERSION_MISSING = "SPEC_VERSION_MISSING"
    SPEC_VERSION_UNSUPPORTED = "SPEC_VERSION_UNSUPPORTED"
    SPEC_UNKNOWN_FIELD = "SPEC_UNKNOWN_FIELD"
    SPEC_MALFORMED = "SPEC_MALFORMED"
    AGENT_NAME_INVALID = "AGENT_NAME_INVALID"
    AGENT_NAME_DUPLICATE = "AGENT_NAME_DUPLICATE"

    # Output
    OUTPUT_SCHEMA_INVALID = "OUTPUT_SCHEMA_INVALID"
    OUTPUT_TYPE_REF_UNRESOLVABLE = "OUTPUT_TYPE_REF_UNRESOLVABLE"
    OUTPUT_TYPE_REF_UNSUPPORTED = "OUTPUT_TYPE_REF_UNSUPPORTED"
    ON_OUTPUT_USECASE_UNKNOWN = "ON_OUTPUT_USECASE_UNKNOWN"
    ON_OUTPUT_INPUT_UNSATISFIED = "ON_OUTPUT_INPUT_UNSATISFIED"
    ON_OUTPUT_USECASE_ALSO_GRANTED = "ON_OUTPUT_USECASE_ALSO_GRANTED"
    ON_OUTPUT_INVOKER_MISSING = "ON_OUTPUT_INVOKER_MISSING"

    # Capabilities
    CAPABILITY_KIND_UNSUPPORTED = "CAPABILITY_KIND_UNSUPPORTED"
    CAPABILITY_EMPTY = "CAPABILITY_EMPTY"
    USECASE_KEY_UNKNOWN = "USECASE_KEY_UNKNOWN"
    SQL_CONNECTION_UNKNOWN = "SQL_CONNECTION_UNKNOWN"
    SQL_CONNECTION_NOT_READONLY = "SQL_CONNECTION_NOT_READONLY"
    SQL_CONFIG_MISSING = "SQL_CONFIG_MISSING"
    SQL_CONNECTION_ROLES_UNBOUND = "SQL_CONNECTION_ROLES_UNBOUND"
    SQL_RESULT_BOUND_MISSING = "SQL_RESULT_BOUND_MISSING"
    MCP_SERVER_UNKNOWN = "MCP_SERVER_UNKNOWN"
    MCP_URL_INVALID = "MCP_URL_INVALID"
    MCP_CREDENTIALS_INLINE = "MCP_CREDENTIALS_INLINE"
    MCP_HEADERS_REF_INVALID = "MCP_HEADERS_REF_INVALID"
    MCP_AUTH_CONFLICT = "MCP_AUTH_CONFLICT"
    MCP_AUTH_STRATEGY_UNKNOWN = "MCP_AUTH_STRATEGY_UNKNOWN"
    MCP_AUTH_STRATEGY_INVALID = "MCP_AUTH_STRATEGY_INVALID"
    SKILLS_LIBRARY_INVALID = "SKILLS_LIBRARY_INVALID"
    SKILLS_LIBRARY_ESCAPES = "SKILLS_LIBRARY_ESCAPES"
    SKILLS_NAME_COLLISION = "SKILLS_NAME_COLLISION"
    SKILLS_ROOT_MISSING = "SKILLS_ROOT_MISSING"
    PYTHON_FACTORY_UNRESOLVABLE = "PYTHON_FACTORY_UNRESOLVABLE"
    PYTHON_FACTORY_NOT_CALLABLE = "PYTHON_FACTORY_NOT_CALLABLE"
    A2A_AGENT_UNKNOWN = "A2A_AGENT_UNKNOWN"
    A2A_URL_INVALID = "A2A_URL_INVALID"
    ANONYMOUS_WITH_DATA_CAPABILITY = "ANONYMOUS_WITH_DATA_CAPABILITY"

    # Model and policy
    MODEL_ROLE_UNBOUND = "MODEL_ROLE_UNBOUND"
    INFERENCE_TARGET_INCOMPLETE = "INFERENCE_TARGET_INCOMPLETE"
    OUTPUT_MODE_UNKNOWN = "OUTPUT_MODE_UNKNOWN"
    POLICY_OUT_OF_RANGE = "POLICY_OUT_OF_RANGE"

    # Deployment resolution
    ENGINE_NOT_FOUND = "ENGINE_NOT_FOUND"
    ENGINE_DUPLICATE = "ENGINE_DUPLICATE"
    ENGINE_API_MISMATCH = "ENGINE_API_MISMATCH"
    PROVIDER_NOT_INSTALLED = "PROVIDER_NOT_INSTALLED"
    PROVIDER_UNKNOWN = "PROVIDER_UNKNOWN"
    PROVIDER_SETTING_MISSING = "PROVIDER_SETTING_MISSING"
    MCP_SERVER_UNREACHABLE = "MCP_SERVER_UNREACHABLE"
    TOOL_FILTER_MATCHES_NOTHING = "TOOL_FILTER_MATCHES_NOTHING"
    SQL_READONLY_DRIFT = "SQL_READONLY_DRIFT"
    ENDPOINT_AUTH_MISSING = "ENDPOINT_AUTH_MISSING"
    A2A_BASE_URL_INVALID = "A2A_BASE_URL_INVALID"
    A2A_EXPOSE_EMPTY = "A2A_EXPOSE_EMPTY"
    AUTH_EXCLUSION_OVERLAPS_AGENTS = "AUTH_EXCLUSION_OVERLAPS_AGENTS"
    A2A_AGENT_UNREACHABLE = "A2A_AGENT_UNREACHABLE"
    AGENT_SPECS_CONFLICT = "AGENT_SPECS_CONFLICT"
    AGENT_SPECS_MISSING = "AGENT_SPECS_MISSING"

    # Compatibility
    SPEC_VERSION_DEPRECATED = "SPEC_VERSION_DEPRECATED"
    UNSPECIFIED = "UNSPECIFIED"


class AgentCompilationIssue(LoomFrozenStruct, frozen=True, kw_only=True):
    """One structured agent compilation failure.

    Args:
        code:      Machine-readable :class:`AgentErrorCode`.
        message:   Human-readable description; the aggregated exception text is
                   built from these messages.
        component: Artifact, agent or config section the issue points at
                   (for example ``"agents/triage.agent.yaml"``).
        field:     Optional field path involved (for example
                   ``"capabilities[0].url"``).
    """

    code: AgentErrorCode
    message: str
    component: str = ""
    field: str | None = None


def from_message(message: str) -> AgentCompilationIssue:
    """Wrap a bare string as an :data:`AgentErrorCode.UNSPECIFIED` issue."""
    return AgentCompilationIssue(code=AgentErrorCode.UNSPECIFIED, message=message)


class AgentCompilationError(Exception):
    """Raised when one or more agent artifacts fail to compile.

    Aggregates every :class:`AgentCompilationIssue` found in a compilation run
    so a generator sees the whole picture instead of one failure at a time.

    Attributes:
        issues: Structured issues, one per failure.

    Args:
        issues: Issues (or bare message strings) collected by the compiler.
            Strings are normalised through :func:`from_message`.
    """

    def __init__(self, issues: Sequence[AgentCompilationIssue | str]) -> None:
        self.issues: tuple[AgentCompilationIssue, ...] = tuple(
            from_message(item) if isinstance(item, str) else item for item in issues
        )
        messages = [issue.message for issue in self.issues]
        super().__init__(
            f"Agent compilation failed with {len(messages)} error(s): {'; '.join(messages)}"
        )


# ---------------------------------------------------------------------------
# Envelope and structure factories
# ---------------------------------------------------------------------------


def spec_version_missing(component: str) -> AgentCompilationIssue:
    """An artifact declares no ``spec_version``, so it cannot be routed."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SPEC_VERSION_MISSING,
        message=f"{component}: spec_version is missing; it must be declared first",
        component=component,
        field="spec_version",
    )


def spec_version_unsupported(
    component: str,
    found: int,
    supported: Sequence[int],
) -> AgentCompilationIssue:
    """The declared spec version is not understood by this release."""
    known = ", ".join(str(version) for version in supported)
    return AgentCompilationIssue(
        code=AgentErrorCode.SPEC_VERSION_UNSUPPORTED,
        message=f"{component}: spec_version {found} is not supported; supported versions: {known}",
        component=component,
        field="spec_version",
    )


def spec_unknown_field(component: str, field: str) -> AgentCompilationIssue:
    """An unrecognised field appeared in the artifact; it is never ignored."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SPEC_UNKNOWN_FIELD,
        message=f"{component}: unknown field '{field}'; unknown fields are rejected",
        component=component,
        field=field,
    )


def spec_malformed(
    component: str,
    reason: str,
    field: str | None = None,
) -> AgentCompilationIssue:
    """The artifact is not decodable as the version it declares."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SPEC_MALFORMED,
        message=f"{component}: malformed artifact: {reason}",
        component=component,
        field=field,
    )


def agent_name_invalid(component: str, reason: str) -> AgentCompilationIssue:
    """The agent name does not satisfy the published name pattern."""
    return AgentCompilationIssue(
        code=AgentErrorCode.AGENT_NAME_INVALID,
        message=f"{component}: invalid agent name: {reason}",
        component=component,
        field="name",
    )


def agent_name_duplicate(name: str, sources: Sequence[str]) -> AgentCompilationIssue:
    """Two artifacts in the same application declare the same agent name.

    Args:
        name: The duplicated agent name.
        sources: Artifact paths that declare it, one entry per occurrence.
    """
    return AgentCompilationIssue(
        code=AgentErrorCode.AGENT_NAME_DUPLICATE,
        message=f"agent '{name}' is declared more than once: {', '.join(sources)}",
        component=name,
        field="name",
    )


# ---------------------------------------------------------------------------
# Output factories
# ---------------------------------------------------------------------------


def output_schema_invalid(component: str, reason: str) -> AgentCompilationIssue:
    """The declared output schema is not a valid JSON Schema object."""
    return AgentCompilationIssue(
        code=AgentErrorCode.OUTPUT_SCHEMA_INVALID,
        message=f"{component}: output schema is not a valid JSON Schema: {reason}",
        component=component,
        field="output.schema",
    )


def output_type_ref_unresolvable(component: str, ref: str) -> AgentCompilationIssue:
    """The ``module:Symbol`` output reference cannot be imported."""
    return AgentCompilationIssue(
        code=AgentErrorCode.OUTPUT_TYPE_REF_UNRESOLVABLE,
        message=f"{component}: output type reference '{ref}' cannot be imported",
        component=component,
        field="output.ref",
    )


def output_type_ref_unsupported(component: str, ref: str, reason: str) -> AgentCompilationIssue:
    """The output reference resolves to a type the engine cannot use."""
    return AgentCompilationIssue(
        code=AgentErrorCode.OUTPUT_TYPE_REF_UNSUPPORTED,
        message=f"{component}: output type reference '{ref}' is unsupported: {reason}",
        component=component,
        field="output.ref",
    )


_ON_OUTPUT_USECASE_FIELD: Final[str] = "on_output.usecase"
"""Spec field every ``on_output`` compilation issue points at."""


def on_output_usecase_unknown(component: str, key: str) -> AgentCompilationIssue:
    """The output hook names a use-case key absent from the registry."""
    return AgentCompilationIssue(
        code=AgentErrorCode.ON_OUTPUT_USECASE_UNKNOWN,
        message=f"{component}: on_output use case '{key}' is not registered",
        component=component,
        field=_ON_OUTPUT_USECASE_FIELD,
    )


def on_output_input_unsatisfied(component: str, key: str, reason: str) -> AgentCompilationIssue:
    """The hook cannot build the use case's Input from the run context and output."""
    return AgentCompilationIssue(
        code=AgentErrorCode.ON_OUTPUT_INPUT_UNSATISFIED,
        message=(f"{component}: on_output use case '{key}' cannot be fed from the run: {reason}"),
        component=component,
        field=_ON_OUTPUT_USECASE_FIELD,
    )


def on_output_usecase_also_granted(component: str, key: str) -> AgentCompilationIssue:
    """The hook's use case is also granted to the model as a capability."""
    return AgentCompilationIssue(
        code=AgentErrorCode.ON_OUTPUT_USECASE_ALSO_GRANTED,
        message=(
            f"{component}: on_output use case '{key}' is also granted as a capability; "
            "a hook use case must not be callable by the model"
        ),
        component=component,
        field=_ON_OUTPUT_USECASE_FIELD,
    )


def on_output_invoker_missing(
    agents: Sequence[str], *, reason: str = "no use-case invoker is configured"
) -> AgentCompilationIssue:
    """Agents declare an output hook but the deployment has no usable use-case invoker.

    Args:
        agents: Names of the agents declaring a hook.
        reason: What is wrong with the invoker, when it is not simply absent.
    """
    return AgentCompilationIssue(
        code=AgentErrorCode.ON_OUTPUT_INVOKER_MISSING,
        message=f"agents declare an output hook but {reason}: {', '.join(agents)}",
        component="ai",
        field="on_output",
    )


# ---------------------------------------------------------------------------
# Capability factories
# ---------------------------------------------------------------------------


def capability_kind_unsupported(component: str, kind: str, engine: str) -> AgentCompilationIssue:
    """The configured engine does not serve this capability kind."""
    return AgentCompilationIssue(
        code=AgentErrorCode.CAPABILITY_KIND_UNSUPPORTED,
        message=f"{component}: engine '{engine}' does not support capability kind '{kind}'",
        component=component,
        field="capabilities",
    )


def capability_empty(component: str, kind: str) -> AgentCompilationIssue:
    """A capability entry grants nothing at all."""
    return AgentCompilationIssue(
        code=AgentErrorCode.CAPABILITY_EMPTY,
        message=f"{component}: capability '{kind}' grants no tool",
        component=component,
        field="capabilities",
    )


def usecase_key_unknown(component: str, key: str) -> AgentCompilationIssue:
    """A granted use-case key is absent from the use-case registry."""
    return AgentCompilationIssue(
        code=AgentErrorCode.USECASE_KEY_UNKNOWN,
        message=f"{component}: use-case key '{key}' is not registered",
        component=component,
        field="capabilities.keys",
    )


def sql_connection_unknown(component: str, connection: str) -> AgentCompilationIssue:
    """A ``sql`` capability names a connection that is not configured."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SQL_CONNECTION_UNKNOWN,
        message=f"{component}: sql connection '{connection}' is not configured",
        component=component,
        field="capabilities.connection",
    )


def sql_connection_not_readonly(component: str, connection: str) -> AgentCompilationIssue:
    """A ``sql`` capability names a connection that permits writes."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SQL_CONNECTION_NOT_READONLY,
        message=f"{component}: sql connection '{connection}' is not read-only",
        component=component,
        field="capabilities.connection",
    )


def sql_config_missing(component: str) -> AgentCompilationIssue:
    """A ``sql`` capability was declared with no data-layer config to validate."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SQL_CONFIG_MISSING,
        message=f"{component}: sql capability declared with no data-layer configuration",
        component=component,
        field="capabilities.connection",
    )


def sql_connection_roles_unbound(component: str, connection: str) -> AgentCompilationIssue:
    """The connection's roles cannot be bound to a caller identity."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SQL_CONNECTION_ROLES_UNBOUND,
        message=(
            f"{component}: roles of sql connection '{connection}' cannot be bound "
            f"to a caller identity"
        ),
        component=component,
        field="capabilities.connection",
    )


def sql_result_bound_missing(component: str, connection: str) -> AgentCompilationIssue:
    """A ``sql`` capability declares no result bounds."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SQL_RESULT_BOUND_MISSING,
        message=(
            f"{component}: sql connection '{connection}' declares no max_rows / "
            f"max_result_bytes bound"
        ),
        component=component,
        field="capabilities.max_rows",
    )


def mcp_server_unknown(component: str, server: str) -> AgentCompilationIssue:
    """An ``mcp`` capability names a server that is not configured."""
    return AgentCompilationIssue(
        code=AgentErrorCode.MCP_SERVER_UNKNOWN,
        message=f"{component}: mcp server '{server}' is not configured in ai.mcp_servers",
        component=component,
        field="capabilities.server",
    )


def mcp_url_invalid(component: str, url: str, reason: str) -> AgentCompilationIssue:
    """An MCP server URL is malformed, not ``https://``, or carries credentials."""
    return AgentCompilationIssue(
        code=AgentErrorCode.MCP_URL_INVALID,
        message=f"{component}: invalid mcp url '{url}': {reason}",
        component=component,
        field="url",
    )


def mcp_credentials_inline(component: str, field: str) -> AgentCompilationIssue:
    """Credentials were written into the artifact instead of being referenced."""
    return AgentCompilationIssue(
        code=AgentErrorCode.MCP_CREDENTIALS_INLINE,
        message=(
            f"{component}: '{field}' carries inline credentials; reference them "
            f"through deployment configuration instead"
        ),
        component=component,
        field=field,
    )


def mcp_headers_ref_invalid(component: str) -> AgentCompilationIssue:
    """A resolved ``headers_ref`` payload is not one ``Name=value`` header pair."""
    return AgentCompilationIssue(
        code=AgentErrorCode.MCP_HEADERS_REF_INVALID,
        message=(
            f"{component}: 'headers_ref' must resolve to one 'Name=value' header "
            f"pair; use an 'auth' strategy for anything richer"
        ),
        component=component,
        field="headers_ref",
    )


def mcp_auth_conflict(component: str) -> AgentCompilationIssue:
    """A server sets both ``headers_ref`` and ``auth``, two credentials for one connection."""
    return AgentCompilationIssue(
        code=AgentErrorCode.MCP_AUTH_CONFLICT,
        message=(
            f"{component}: 'headers_ref' and 'auth' are mutually exclusive; "
            f"one connection carries one credential"
        ),
        component=component,
        field="auth",
    )


def mcp_auth_strategy_unknown(
    component: str, kind: str, available: Sequence[str]
) -> AgentCompilationIssue:
    """A named auth strategy resolves to no entry point in ``loom.ai.remote_auth``."""
    installed = ", ".join(available) if available else "none"
    return AgentCompilationIssue(
        code=AgentErrorCode.MCP_AUTH_STRATEGY_UNKNOWN,
        message=(
            f"{component}: auth strategy '{kind}' is not registered in entry-point "
            f"group 'loom.ai.remote_auth'; registered: {installed}"
        ),
        component=component,
        field="auth.kind",
    )


def mcp_auth_strategy_invalid(kind: str, reason: str) -> AgentCompilationIssue:
    """A registered auth strategy could not be constructed, or is unusable."""
    return AgentCompilationIssue(
        code=AgentErrorCode.MCP_AUTH_STRATEGY_INVALID,
        message=f"auth strategy '{kind}' is unusable: {reason}",
        component=f"loom.ai.remote_auth:{kind}",
        field="auth.kind",
    )


def skills_library_invalid(component: str, library: str, reason: str) -> AgentCompilationIssue:
    """A skill library does not resolve to a readable library directory."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SKILLS_LIBRARY_INVALID,
        message=f"{component}: skills library '{library}' is unusable: {reason}",
        component=component,
        field="capabilities.library",
    )


def skills_library_escapes(component: str, library: str) -> AgentCompilationIssue:
    """A skill library resolves outside the directory it is anchored to."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SKILLS_LIBRARY_ESCAPES,
        message=f"{component}: skills library '{library}' escapes its own directory",
        component=component,
        field="capabilities.library",
    )


def skills_name_collision(
    component: str,
    skill: str,
    first_library: str,
    second_library: str,
) -> AgentCompilationIssue:
    """Two libraries granted to one agent expose the same skill name."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SKILLS_NAME_COLLISION,
        message=(
            f"{component}: skill '{skill}' is exposed by both libraries "
            f"'{first_library}' and '{second_library}'"
        ),
        component=component,
        field="capabilities.library",
    )


def skills_root_missing(component: str) -> AgentCompilationIssue:
    """A bare skill library was named with no ``skills_root`` configured."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SKILLS_ROOT_MISSING,
        message=(
            f"{component}: a bare skills library requires a configured skills_root; "
            f"use './name' to resolve it beside the artifact instead"
        ),
        component=component,
        field="capabilities.library",
    )


def python_factory_unresolvable(component: str, factory: str, reason: str) -> AgentCompilationIssue:
    """A ``python`` capability factory cannot be imported."""
    return AgentCompilationIssue(
        code=AgentErrorCode.PYTHON_FACTORY_UNRESOLVABLE,
        message=f"{component}: python factory '{factory}' cannot be imported: {reason}",
        component=component,
        field="capabilities.factory",
    )


def python_factory_not_callable(component: str, factory: str) -> AgentCompilationIssue:
    """A ``python`` capability factory does not satisfy ``ToolsetFactory``."""
    return AgentCompilationIssue(
        code=AgentErrorCode.PYTHON_FACTORY_NOT_CALLABLE,
        message=f"{component}: python factory '{factory}' does not satisfy ToolsetFactory",
        component=component,
        field="capabilities.factory",
    )


def a2a_agent_unknown(component: str, agent: str) -> AgentCompilationIssue:
    """An ``a2a`` capability names a remote agent that is not configured."""
    return AgentCompilationIssue(
        code=AgentErrorCode.A2A_AGENT_UNKNOWN,
        message=f"{component}: a2a agent '{agent}' is not configured in ai.a2a_agents",
        component=component,
        field="capabilities.agent",
    )


def a2a_url_invalid(component: str, url: str, reason: str) -> AgentCompilationIssue:
    """A remote agent URL is malformed, not ``https://``, or carries credentials."""
    return AgentCompilationIssue(
        code=AgentErrorCode.A2A_URL_INVALID,
        message=f"{component}: invalid a2a url '{url}': {reason}",
        component=component,
        field="url",
    )


def anonymous_with_data_capability(component: str, kind: str) -> AgentCompilationIssue:
    """An unauthenticated agent holds a data or remote capability."""
    return AgentCompilationIssue(
        code=AgentErrorCode.ANONYMOUS_WITH_DATA_CAPABILITY,
        message=(
            f"{component}: agent opts out of authentication while holding the '{kind}' capability"
        ),
        component=component,
        field="capabilities",
    )


# ---------------------------------------------------------------------------
# Model and policy factories
# ---------------------------------------------------------------------------


def model_role_unbound(component: str, role: str) -> AgentCompilationIssue:
    """The agent's model role is not present in ``ai.models``."""
    return AgentCompilationIssue(
        code=AgentErrorCode.MODEL_ROLE_UNBOUND,
        message=f"{component}: model role '{role}' is not bound in ai.models",
        component=component,
        field="model_role",
    )


def inference_target_incomplete(role: str, setting: str) -> AgentCompilationIssue:
    """A model-role binding lacks a setting its provider requires."""
    return AgentCompilationIssue(
        code=AgentErrorCode.INFERENCE_TARGET_INCOMPLETE,
        message=f"model role '{role}': required setting '{setting}' is missing",
        component=f"model role '{role}'",
        field=setting,
    )


def output_mode_unknown(role: str, value: str, valid: Sequence[str]) -> AgentCompilationIssue:
    """A model-role binding names an ``output_mode`` loom does not offer.

    The valid set is a parameter, as in every sibling factory that names one:
    it keeps this module free of domain imports.
    """
    return AgentCompilationIssue(
        code=AgentErrorCode.OUTPUT_MODE_UNKNOWN,
        message=(f"model role '{role}': output_mode '{value}' is not one of {', '.join(valid)}"),
        component=f"model role '{role}'",
        field="output_mode",
    )


def policy_out_of_range(
    component: str,
    policy: str,
    value: int,
    minimum: int,
    maximum: int,
) -> AgentCompilationIssue:
    """A policy value falls outside its documented range."""
    return AgentCompilationIssue(
        code=AgentErrorCode.POLICY_OUT_OF_RANGE,
        message=(
            f"{component}: policy '{policy}' value {value} is outside the "
            f"allowed range {minimum}..{maximum}"
        ),
        component=component,
        field=f"policies.{policy}",
    )


# ---------------------------------------------------------------------------
# Deployment resolution factories
# ---------------------------------------------------------------------------


def engine_not_found(name: str, available: Sequence[str]) -> AgentCompilationIssue:
    """No installed entry point provides the requested engine."""
    known = ", ".join(available) if available else "none"
    return AgentCompilationIssue(
        code=AgentErrorCode.ENGINE_NOT_FOUND,
        message=f"engine '{name}' is not installed; available engines: {known}",
        component=name,
        field="ai.engine",
    )


def engine_duplicate(name: str, distributions: Sequence[str]) -> AgentCompilationIssue:
    """Two distributions claim the same engine entry-point name."""
    return AgentCompilationIssue(
        code=AgentErrorCode.ENGINE_DUPLICATE,
        message=(
            f"engine '{name}' is provided by more than one distribution: {', '.join(distributions)}"
        ),
        component=name,
        field="ai.engine",
    )


def engine_api_mismatch(name: str, found: int, supported: Sequence[int]) -> AgentCompilationIssue:
    """An engine announces a handshake version this release cannot speak."""
    known = ", ".join(str(version) for version in supported)
    return AgentCompilationIssue(
        code=AgentErrorCode.ENGINE_API_MISMATCH,
        message=(f"engine '{name}' speaks handshake version {found}; supported versions: {known}"),
        component=name,
        field="ai.engine",
    )


def provider_not_installed(provider: str, extra: str) -> AgentCompilationIssue:
    """A provider SDK is missing; the message names the extra to install."""
    return AgentCompilationIssue(
        code=AgentErrorCode.PROVIDER_NOT_INSTALLED,
        message=f"provider '{provider}' is not installed; install the '{extra}' extra",
        component=provider,
    )


def provider_unknown(provider: str, supported: Sequence[str]) -> AgentCompilationIssue:
    """The provider is not one this release knows how to bind.

    Distinct from ``PROVIDER_NOT_INSTALLED``: there is no extra to install,
    because no such provider exists in this release.

    Args:
        provider: Provider identifier the artifact named.
        supported: Provider identifiers this release binds.
    """
    return AgentCompilationIssue(
        code=AgentErrorCode.PROVIDER_UNKNOWN,
        message=(
            f"provider '{provider}' is not known to this release of loom; "
            f"supported providers: {', '.join(supported)}"
        ),
        component=provider,
    )


def provider_setting_missing(provider: str, setting: str) -> AgentCompilationIssue:
    """A provider setting (credentials, region, endpoint) is absent."""
    return AgentCompilationIssue(
        code=AgentErrorCode.PROVIDER_SETTING_MISSING,
        message=f"provider '{provider}': required setting '{setting}' is missing",
        component=provider,
        field=setting,
    )


def mcp_server_unreachable(server: str, reason: str) -> AgentCompilationIssue:
    """An MCP server is not reachable at start-up.

    Args:
        server: The server's registered name, never its URL — a URL carries
            credentials and hosts that the redaction guarantee keeps out of
            diagnostics (FR-030a/FR-038).
        reason: Why the connection did not complete.
    """
    return AgentCompilationIssue(
        code=AgentErrorCode.MCP_SERVER_UNREACHABLE,
        message=f"mcp server '{server}' is unreachable: {reason}",
        component=server,
    )


def tool_filter_matches_nothing(component: str, target: str) -> AgentCompilationIssue:
    """An include/exclude filter excludes every tool the target exposes."""
    return AgentCompilationIssue(
        code=AgentErrorCode.TOOL_FILTER_MATCHES_NOTHING,
        message=f"{component}: filter for '{target}' matches no tool",
        component=component,
        field="capabilities.include",
    )


def sql_readonly_drift(connection: str) -> AgentCompilationIssue:
    """Live configuration contradicts the plan's read-only assumption."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SQL_READONLY_DRIFT,
        message=(
            f"sql connection '{connection}' is no longer read-only; the compiled "
            f"plan assumed it was"
        ),
        component=connection,
    )


def endpoint_auth_missing(component: str) -> AgentCompilationIssue:
    """An agent opted into HTTP exposure without naming its authentication."""
    return AgentCompilationIssue(
        code=AgentErrorCode.ENDPOINT_AUTH_MISSING,
        message=f"{component}: HTTP exposure requires a named authentication",
        component=component,
        field="auth",
    )


def a2a_base_url_invalid(url: str, reason: str) -> AgentCompilationIssue:
    """Report an ``ai.a2a.base_url`` that is unsafe to publish.

    Args:
        url: The offending URL, already redacted of userinfo and query.
        reason: Why it is unsafe, in the vocabulary of the URL check.

    Returns:
        The issue, coded :data:`AgentErrorCode.A2A_BASE_URL_INVALID`.
    """
    return AgentCompilationIssue(
        code=AgentErrorCode.A2A_BASE_URL_INVALID,
        component="ai.a2a",
        field="base_url",
        message=f"the published card base URL is unsafe: {reason} ({url})",
    )


def a2a_expose_empty() -> AgentCompilationIssue:
    """A2A exposure was enabled without naming a single agent."""
    return AgentCompilationIssue(
        code=AgentErrorCode.A2A_EXPOSE_EMPTY,
        message="a2a exposure is enabled but names no agent",
        component="a2a",
        field="a2a.expose",
    )


def auth_exclusion_overlaps_agents(paths: Sequence[str]) -> AgentCompilationIssue:
    """An authentication exclusion covers an agent or A2A invocation path."""
    return AgentCompilationIssue(
        code=AgentErrorCode.AUTH_EXCLUSION_OVERLAPS_AGENTS,
        message=(f"authentication exclusions cover agent invocation paths: {', '.join(paths)}"),
        component="auth",
        field="auth.exclude",
    )


def a2a_agent_unreachable(agent: str, reason: str) -> AgentCompilationIssue:
    """A remote agent's card cannot be retrieved.

    Args:
        agent: The remote agent's registered name, never its URL — a URL
            carries credentials and hosts that the redaction guarantee keeps
            out of diagnostics (FR-030a/FR-038).
        reason: Why the card could not be retrieved.
    """
    return AgentCompilationIssue(
        code=AgentErrorCode.A2A_AGENT_UNREACHABLE,
        message=f"remote a2a agent '{agent}' is unreachable: {reason}",
        component=agent,
    )


def agent_specs_conflict() -> AgentCompilationIssue:
    """Both artifact sources declare agents; there is no implicit precedence."""
    return AgentCompilationIssue(
        code=AgentErrorCode.AGENT_SPECS_CONFLICT,
        message=(
            "agent artifacts are declared both by the manifest 'AGENTS' attribute and by "
            "the 'ai.specs' config key; declare them in exactly one of the two"
        ),
        component="ai",
        field="ai.specs",
    )


def agent_specs_missing() -> AgentCompilationIssue:
    """The ``ai:`` section is configured but no artifact source declares agents."""
    return AgentCompilationIssue(
        code=AgentErrorCode.AGENT_SPECS_MISSING,
        message=(
            "the 'ai:' section is configured but declares no agent artifact; set 'ai.specs' "
            "or the manifest 'AGENTS' attribute"
        ),
        component="ai",
        field="ai.specs",
    )


# ---------------------------------------------------------------------------
# Compatibility factories
# ---------------------------------------------------------------------------


def spec_version_deprecated(component: str, found: int, latest: int) -> AgentCompilationIssue:
    """The artifact's version is still accepted but has been superseded."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SPEC_VERSION_DEPRECATED,
        message=(
            f"{component}: spec_version {found} is deprecated; version {latest} is the current one"
        ),
        component=component,
        field="spec_version",
    )


# ---------------------------------------------------------------------------
# Run-time catalogue
# ---------------------------------------------------------------------------


class AgentRunErrorClass(StrEnum):
    """Class of a run-time failure; the retry policy reads this, not the message."""

    INFRASTRUCTURE = "INFRASTRUCTURE"
    MODEL_BEHAVIOUR = "MODEL_BEHAVIOUR"
    LIMIT = "LIMIT"
    AUTHORIZATION = "AUTHORIZATION"
    CLIENT = "CLIENT"
    APPLICATION = "APPLICATION"


class AgentRunErrorCode(StrEnum):
    """Enumeration of all agent run-time failure codes."""

    PROVIDER_UNAVAILABLE = "PROVIDER_UNAVAILABLE"
    PROVIDER_RATE_LIMITED = "PROVIDER_RATE_LIMITED"
    TOOL_TIMEOUT = "TOOL_TIMEOUT"
    TOOL_UNAVAILABLE = "TOOL_UNAVAILABLE"
    OUTPUT_SCHEMA_VIOLATION = "OUTPUT_SCHEMA_VIOLATION"
    MAX_ITERATIONS_EXCEEDED = "MAX_ITERATIONS_EXCEEDED"
    RUN_TIMEOUT = "RUN_TIMEOUT"
    TOO_MANY_RUNS = "TOO_MANY_RUNS"
    UNAUTHORIZED = "UNAUTHORIZED"
    CANCELLED = "CANCELLED"
    HOOK_FAILED = "HOOK_FAILED"


class AgentRunError(Exception):
    """A run failed with a stable, machine-readable code.

    Lives with :class:`AgentRunErrorCode` rather than with the runtime that
    raises it: the engine adapters classify and re-raise it, and importing the
    whole live runtime — its exit stack, its shared sessions, its SQL
    configuration — to reach one exception class would point the dependency
    arrow at the concretion instead of at the contract.

    Args:
        code: Run-time failure code; the retry policy reads its class.
        message: Human-readable description, safe to return to the caller.
        interaction_id: Identifier of the admitted run, when the failure
            happened after admission; ``None`` for pre-admission failures.

    Attributes:
        code: The failure code carried by this error.
        interaction_id: The run this error belongs to, or ``None``.

    Example::

        raise AgentRunError(AgentRunErrorCode.RUN_TIMEOUT, "the run took too long")
    """

    def __init__(
        self, code: AgentRunErrorCode, message: str, *, interaction_id: str | None = None
    ) -> None:
        super().__init__(message)
        self.code = code
        self.interaction_id = interaction_id


_RUN_ERROR_CLASSES: Mapping[AgentRunErrorCode, AgentRunErrorClass] = MappingProxyType(
    {
        AgentRunErrorCode.PROVIDER_UNAVAILABLE: AgentRunErrorClass.INFRASTRUCTURE,
        AgentRunErrorCode.PROVIDER_RATE_LIMITED: AgentRunErrorClass.INFRASTRUCTURE,
        AgentRunErrorCode.TOOL_TIMEOUT: AgentRunErrorClass.INFRASTRUCTURE,
        AgentRunErrorCode.TOOL_UNAVAILABLE: AgentRunErrorClass.INFRASTRUCTURE,
        AgentRunErrorCode.OUTPUT_SCHEMA_VIOLATION: AgentRunErrorClass.MODEL_BEHAVIOUR,
        AgentRunErrorCode.MAX_ITERATIONS_EXCEEDED: AgentRunErrorClass.LIMIT,
        AgentRunErrorCode.RUN_TIMEOUT: AgentRunErrorClass.LIMIT,
        AgentRunErrorCode.TOO_MANY_RUNS: AgentRunErrorClass.LIMIT,
        AgentRunErrorCode.UNAUTHORIZED: AgentRunErrorClass.AUTHORIZATION,
        AgentRunErrorCode.CANCELLED: AgentRunErrorClass.CLIENT,
        AgentRunErrorCode.HOOK_FAILED: AgentRunErrorClass.APPLICATION,
    }
)


def run_error_class(code: AgentRunErrorCode) -> AgentRunErrorClass:
    """Return the failure class of a run-time error code.

    The mapping is total: every member of :class:`AgentRunErrorCode` has an
    entry, so a new code without a class fails immediately instead of silently
    defaulting to a retriable class.

    Args:
        code: Run-time error code to classify.

    Returns:
        The class the retry policy must read.

    Raises:
        KeyError: If the code has no registered class.
    """
    return _RUN_ERROR_CLASSES[code]


def is_retriable(code: AgentRunErrorCode) -> bool:
    """Return whether a run-time failure may be retried by the caller.

    Only :data:`AgentRunErrorClass.INFRASTRUCTURE` failures are retriable;
    model behaviour, limits, authorization, client cancellation and
    application failures are final.

    Args:
        code: Run-time error code to test.

    Returns:
        ``True`` when the code's class is ``INFRASTRUCTURE``.
    """
    return run_error_class(code) is AgentRunErrorClass.INFRASTRUCTURE
