"""Capability phase: one handler per ``kind``, dispatched by a map.

Every grant is validated statically and resolved to a handle: the registered
use-case types, the SQL connection config, the imported factory.  URLs are the
declared exception — they resolve over the network in ``__aenter__``
(invariant 3), so only their well-formedness is checked here.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Final
from urllib.parse import SplitResult, urlsplit

from loom.ai.compiler._plan import (
    CompiledA2ACapability,
    CompiledCapability,
    CompiledMcpCapability,
    CompiledPythonCapability,
    CompiledSkillsCapability,
    CompiledSqlCapability,
    CompiledUsecaseCapability,
)
from loom.ai.compiler._symbols import import_symbol

# Private cross-module reuse inside the pillar: the fail-closed "reference,
# not secret" heuristic is defined once, next to the config it protects.
from loom.ai.config import AiConfig, _is_credentials_reference
from loom.ai.declarative import (
    A2ACapability,
    AgentSpecV1,
    CapabilitySpec,
    McpCapability,
    PythonCapability,
    SkillsCapability,
    SqlCapability,
    UsecaseCapability,
)
from loom.ai.errors import (
    AgentCompilationIssue,
    a2a_url_invalid,
    anonymous_with_data_capability,
    capability_kind_unsupported,
    mcp_credentials_inline,
    mcp_url_invalid,
    python_factory_not_callable,
    python_factory_unresolvable,
    skills_ref_invalid,
    skills_root_missing,
    sql_config_missing,
    sql_connection_not_readonly,
    sql_connection_roles_unbound,
    sql_connection_unknown,
    sql_result_bound_missing,
    usecase_key_unknown,
)
from loom.core.engine.compilable import Compilable
from loom.core.sql.config import SqlConfig, roles_need_identity_binding
from loom.core.use_case.registry import UseCaseRegistry

_URL_USERINFO_RE = re.compile(r"://[^/]*@")

# Kinds that read application data or call application/remote code.  An agent
# that opts out of authentication may hold none of them (FR-045a); ``skills``
# only injects packaged prompt material, so it is exempt.
_DATA_OR_REMOTE_KINDS: Final[frozenset[str]] = frozenset({"usecase", "sql", "python", "mcp", "a2a"})

_CompileResult = tuple[tuple[CompiledCapability, ...], list[AgentCompilationIssue]]
_HandlerResult = tuple[CompiledCapability | None, list[AgentCompilationIssue]]


@dataclass(frozen=True)
class _Context:
    """Deployment inputs one capability handler may consult."""

    component: str
    registry: UseCaseRegistry
    sql: SqlConfig | None
    skills_root: str | None
    anonymous: bool


def compile_capabilities(
    spec: AgentSpecV1,
    *,
    component: str,
    config: AiConfig,
    registry: UseCaseRegistry,
    sql: SqlConfig | None,
    supported_kinds: frozenset[str],
) -> _CompileResult:
    """Validate every declared capability and resolve it to a handle.

    Args:
        spec: Decoded artifact whose capabilities are compiled.
        component: Artifact path or agent name the issues point at.
        config: Deployment configuration (engine name, skills root, endpoints).
        registry: Use-case registry the ``usecase`` grants resolve against.
        sql: Data-layer configuration; ``None`` fails every ``sql`` grant.
        supported_kinds: Kinds the configured engine serves, as a plain value.

    Returns:
        The compiled capabilities and every issue found across all of them.
    """
    context = _Context(
        component=component,
        registry=registry,
        sql=sql,
        skills_root=config.skills_root,
        anonymous=_is_anonymous(spec.name, config),
    )
    compiled: list[CompiledCapability] = []
    issues: list[AgentCompilationIssue] = []
    for capability in spec.capabilities:
        kind = str(capability.__struct_config__.tag)
        if kind not in supported_kinds:
            issues.append(capability_kind_unsupported(component, kind, config.engine))
            continue
        if context.anonymous and kind in _DATA_OR_REMOTE_KINDS:
            issues.append(anonymous_with_data_capability(component, kind))
        item, item_issues = _HANDLERS[type(capability)](capability, context)
        issues.extend(item_issues)
        if item is not None:
            compiled.append(item)
    return tuple(compiled), issues


def _is_anonymous(agent_name: str, config: AiConfig) -> bool:
    endpoint = config.endpoints.get(agent_name)
    return endpoint is not None and endpoint.allow_anonymous


def _compile_usecase(capability: UsecaseCapability, context: _Context) -> _HandlerResult:
    issues: list[AgentCompilationIssue] = []
    use_cases: list[type[Compilable]] = []
    for key in capability.keys:
        try:
            use_cases.append(context.registry.resolve(key))
        except KeyError:
            issues.append(usecase_key_unknown(context.component, key))
    if issues:
        return None, issues
    return CompiledUsecaseCapability(keys=capability.keys, use_cases=tuple(use_cases)), []


def _compile_sql(capability: SqlCapability, context: _Context) -> _HandlerResult:
    if context.sql is None:
        return None, [sql_config_missing(context.component)]
    connection = context.sql.connections.get(capability.connection)
    if connection is None:
        return None, [sql_connection_unknown(context.component, capability.connection)]
    issues: list[AgentCompilationIssue] = []
    if not connection.readonly:
        issues.append(sql_connection_not_readonly(context.component, capability.connection))
    binds_roles = not context.anonymous
    if roles_need_identity_binding(connection.allowed_roles, mechanism_binds_roles=binds_roles):
        issues.append(sql_connection_roles_unbound(context.component, capability.connection))
    # Defensive: the v1 struct already makes absent or non-positive bounds
    # unrepresentable, but a spec built through another path must not pass.
    if capability.max_rows < 1 or capability.max_result_bytes < 1:
        issues.append(sql_result_bound_missing(context.component, capability.connection))
    if issues:
        return None, issues
    return (
        CompiledSqlCapability(
            connection=capability.connection,
            config=connection,
            max_rows=capability.max_rows,
            max_result_bytes=capability.max_result_bytes,
        ),
        [],
    )


def _compile_mcp(capability: McpCapability, context: _Context) -> _HandlerResult:
    issues: list[AgentCompilationIssue] = []
    fault = _url_fault(capability.url)
    if fault is not None:
        issues.append(mcp_url_invalid(context.component, _redact_url(capability.url), fault))
    headers_ref = capability.headers_ref
    if headers_ref is not None and not _is_credentials_reference(headers_ref):
        issues.append(mcp_credentials_inline(context.component, "capabilities.headers_ref"))
    if issues:
        return None, issues
    return (
        CompiledMcpCapability(
            url=capability.url,
            tool_filter=capability.tool_filter,
            headers_ref=headers_ref,
        ),
        [],
    )


def _compile_skills(capability: SkillsCapability, context: _Context) -> _HandlerResult:
    issues: list[AgentCompilationIssue] = []
    if context.skills_root is None:
        issues.append(skills_root_missing(context.component))
    skills: list[object] = []
    for ref in capability.refs:
        try:
            skills.append(import_symbol(ref))
        except (ImportError, AttributeError, ValueError):
            issues.append(skills_ref_invalid(context.component, ref))
    if issues:
        return None, issues
    return CompiledSkillsCapability(refs=capability.refs, skills=tuple(skills)), []


def _compile_python(capability: PythonCapability, context: _Context) -> _HandlerResult:
    try:
        factory = import_symbol(capability.factory)
    except (ImportError, AttributeError, ValueError) as exc:
        return None, [python_factory_unresolvable(context.component, capability.factory, str(exc))]
    if not callable(factory):
        return None, [python_factory_not_callable(context.component, capability.factory)]
    return CompiledPythonCapability(factory_ref=capability.factory, factory=factory), []


def _compile_a2a(capability: A2ACapability, context: _Context) -> _HandlerResult:
    fault = _url_fault(capability.url)
    if fault is not None:
        return None, [a2a_url_invalid(context.component, _redact_url(capability.url), fault)]
    return CompiledA2ACapability(url=capability.url, skills=capability.skills), []


def _url_fault(url: str) -> str | None:
    """Return why a remote URL is unsafe, or ``None`` when it is acceptable."""
    try:
        parts: SplitResult = urlsplit(url)
    except ValueError:
        return "the URL is malformed"
    if parts.scheme != "https":
        return "the scheme must be https"
    if not parts.hostname:
        return "the URL declares no host"
    if parts.username is not None or parts.password is not None:
        return "the URL carries credentials in its userinfo"
    if parts.query:
        return "the URL carries a query string, which may embed credentials"
    return None


def _redact_url(url: str) -> str:
    """Strip userinfo and query so an invalid-URL message cannot leak a secret."""
    return _URL_USERINFO_RE.sub("://***@", url).split("?", 1)[0]


# Dispatch map keyed by the declared capability type.  ``Any`` in the handler
# parameter is the dispatch boundary: each handler is statically typed for its
# own capability, and the map guarantees the pairing.
_HANDLERS: Final[Mapping[type[CapabilitySpec], Callable[[Any, _Context], _HandlerResult]]] = {
    UsecaseCapability: _compile_usecase,
    SqlCapability: _compile_sql,
    McpCapability: _compile_mcp,
    SkillsCapability: _compile_skills,
    PythonCapability: _compile_python,
    A2ACapability: _compile_a2a,
}
