"""Typed configuration for the ``ai:`` deployment section.

Parsed via ``ctx.section(ConfigKey.AI, AiConfig)`` through the existing
:mod:`loom.core.config` loader, so ``${oc.env:...}`` interpolations and the
secret resolver apply before the decode, unchanged.  Validation is fail-fast
per struct and every issue carries a stable
:class:`~loom.ai.errors.AgentErrorCode`; :class:`AiConfig` aggregates the
issues found across every model role and endpoint and raises once.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from types import MappingProxyType

from msgspec import field

from loom.ai.errors import (
    AgentCompilationError,
    AgentCompilationIssue,
    a2a_expose_empty,
    endpoint_auth_missing,
    inference_target_incomplete,
    mcp_credentials_inline,
)
from loom.ai.inference import InferenceTarget
from loom.core.model import LoomFrozenStruct

# Conservative allowlist for a secret *reference*: a name, path, ARN or key id
# the secrets resolver can look up.  It excludes whitespace, newlines, quotes,
# braces (so JSON blobs and PEM blocks never pass) — anything outside the
# pattern is rejected, fail-closed.
_REFERENCE_PATTERN = re.compile(r"^[A-Za-z0-9_/.:@=+-]+$")

# Known shapes of literal secret material that would otherwise satisfy the
# reference pattern: AWS access key ids, OpenAI-style keys, GitHub tokens.
_SECRET_MATERIAL_PREFIXES = ("AKIA", "sk-", "ghp_")

# A URL carrying userinfo (``scheme://user:pass@host``) embeds a credential.
_URL_USERINFO_RE = re.compile(r"://[^/]*@")

# Settings a provider requires directly in its ``InferenceTarget`` binding.
# Providers absent from this map need nothing beyond ``provider``/``model``
# at config time; their SDK-level settings are validated by the engine via
# ``loom.ai.registry.require_provider_setting``.  ``gateway`` designates a
# custom OpenAI-compatible gateway and therefore requires its ``endpoint``.
_REQUIRED_SETTINGS_BY_PROVIDER: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "bedrock": ("region",),
        "gateway": ("endpoint",),
    }
)


def _is_credentials_reference(value: str) -> bool:
    """Report whether ``value`` looks like a secret reference, not a secret.

    Fail-closed heuristic (FR-018): the value must match the conservative
    reference allowlist (which already excludes spaces, newlines, ``{`` and
    therefore JSON blobs and ``BEGIN PRIVATE KEY`` blocks) and must not match
    known literal-secret shapes or a URL with userinfo.
    """
    if _REFERENCE_PATTERN.fullmatch(value) is None:
        return False
    if value.startswith(_SECRET_MATERIAL_PREFIXES):
        return False
    return _URL_USERINFO_RE.search(value) is None


def _validate_model_binding(role: str, target: InferenceTarget) -> list[AgentCompilationIssue]:
    """Collect the issues of one ``ai.models.<role>`` binding."""
    issues: list[AgentCompilationIssue] = []
    required = _REQUIRED_SETTINGS_BY_PROVIDER.get(target.provider, ())
    for setting in required:
        if not getattr(target, setting):
            issues.append(inference_target_incomplete(role, setting))
    ref = target.credentials_ref
    if ref is not None and not _is_credentials_reference(ref):
        # The rejected value is deliberately absent from the issue: the error
        # message must not leak the very secret it rejects.
        issues.append(mcp_credentials_inline(f"model role '{role}'", "credentials_ref"))
    return issues


class AgentEndpointConfig(LoomFrozenStruct, frozen=True, kw_only=True):
    """Per-agent HTTP exposure opt-in (FR-029a, FR-045a).

    An agent absent from ``ai.endpoints`` is never mounted; presence requires
    naming the authentication explicitly — there is no default.

    Attributes:
        enabled: Whether to mount the agent's HTTP endpoints.
        auth: Named authentication the mount requires.  Mandatory.
        allow_anonymous: Whether an empty-subject identity is accepted for
            this agent (FR-045a).
    """

    enabled: bool
    auth: str
    allow_anonymous: bool = False


class A2AConfig(LoomFrozenStruct, frozen=True, kw_only=True):
    """A2A exposure settings (FR-041).

    Attributes:
        base_url: Public URL the agent card advertises.
        expose: Agent names to publish.  Must be non-empty: empty means none,
            never all (FR-041a).

    Raises:
        AgentCompilationError: With :data:`~loom.ai.errors.AgentErrorCode.A2A_EXPOSE_EMPTY`
            when ``expose`` names no agent.
    """

    base_url: str
    expose: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.expose:
            raise AgentCompilationError([a2a_expose_empty()])


class AiConfig(LoomFrozenStruct, frozen=True, kw_only=True):
    """Deployment configuration of the AI pillar (``ai:`` section).

    Attributes:
        engine: Entry-point name in group ``loom.ai.engines``.
        specs: Glob patterns of agent artifacts, relative to the app root.
        models: Model-role bindings; must contain every role an agent declares.
        skills_root: Package root for ``kind: skills`` capabilities.
        a2a: A2A exposure; absent means no card and no A2A endpoints (FR-041).
        endpoints: Per-agent HTTP opt-in (FR-029a).
        startup_timeout_ms: Per live client while opening them concurrently.
        max_concurrent_runs: Per-worker run limit (FR-033a).
        max_prompt_bytes: Enforced while reading the request body.
        health_cache_ttl_ms: Refresh period of the health probe.

    Raises:
        AgentCompilationError: Aggregating one issue per invalid model binding
            (incomplete provider settings, literal secret in
            ``credentials_ref``) and per endpoint without a named ``auth``.
    """

    engine: str
    specs: tuple[str, ...]
    models: dict[str, InferenceTarget]
    skills_root: str | None = None
    a2a: A2AConfig | None = None
    endpoints: dict[str, AgentEndpointConfig] = field(default_factory=dict)
    startup_timeout_ms: int = 10000
    max_concurrent_runs: int = 8
    max_prompt_bytes: int = 65536
    health_cache_ttl_ms: int = 5000

    def __post_init__(self) -> None:
        issues: list[AgentCompilationIssue] = []
        for role, target in self.models.items():
            issues.extend(_validate_model_binding(role, target))
        for name, endpoint in self.endpoints.items():
            if not endpoint.auth.strip():
                issues.append(endpoint_auth_missing(name))
        if issues:
            raise AgentCompilationError(issues)
