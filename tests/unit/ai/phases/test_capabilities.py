"""Capability phase failures (T050): one case per applicable error code.

Documented decisions taken while writing these tests red:

* ``SKILLS_REF_INVALID`` — the ``_v1`` pattern already makes absolute paths
  unrepresentable at decode, so the remaining static fault the phase can find
  is a ``module:symbol`` reference that does not import; that is the case
  exercised here.
* ``SQL_CONNECTION_ROLES_UNBOUND`` — semantics anchored on
  ``loom.core.sql.config.roles_need_identity_binding``: a connection whose
  ``allowed_roles`` is non-empty needs a verified caller identity to bind a
  subset of the allowlist. An agent whose endpoint opts into
  ``allow_anonymous`` carries no such identity, so granting it that
  connection must be rejected (FR-043a). The anonymous opt-out also trips
  ``ANONYMOUS_WITH_DATA_CAPABILITY``, hence the containment assertion.
* ``SQL_RESULT_BOUND_MISSING`` — see ``TestSqlResultBoundMissing``.
* Anonymous classification (FR-045a): kinds that read application data or
  call a remote system — ``usecase``, ``sql``, ``python``, ``mcp``, ``a2a`` —
  trip ``ANONYMOUS_WITH_DATA_CAPABILITY``; ``skills`` grants packaged prompt
  material only, reads no data and calls nothing remote, so it does not.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import msgspec
import pytest

import loom.ai.compiler  # noqa: F401  — red until the compiler exists
from loom.ai.config import AgentEndpointConfig, AiConfig
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
from loom.ai.errors import AgentCompilationIssue, AgentErrorCode

_BAD_URLS: list[str] = [
    "not a url",
    "http://tools.example.com/mcp",
    "https://user:pass@tools.example.com/mcp",
    "https://tools.example.com/mcp?token=abc",
]
_BAD_URL_IDS: list[str] = ["malformed", "plain_http", "userinfo_credentials", "query_credentials"]


def _valid_sql() -> SqlCapability:
    return SqlCapability(connection="reporting_readonly", max_rows=500, max_result_bytes=1_048_576)


def test_reports_kind_unsupported_when_engine_does_not_serve_the_kind(
    spec_factory: Callable[..., AgentSpecV1],
    single_issue_for: Callable[..., AgentCompilationIssue],
) -> None:
    spec = spec_factory(capabilities=(McpCapability(url="https://tools.example.com/mcp"),))
    issue = single_issue_for(spec, supported_kinds=frozenset({"usecase"}))
    assert (issue.code, issue.field) == (
        AgentErrorCode.CAPABILITY_KIND_UNSUPPORTED,
        "capabilities",
    )


def test_reports_usecase_key_unknown_when_key_is_not_registered(
    spec_factory: Callable[..., AgentSpecV1],
    single_issue_for: Callable[..., AgentCompilationIssue],
) -> None:
    spec = spec_factory(capabilities=(UsecaseCapability(keys=("nowhere.unknown_key",)),))
    issue = single_issue_for(spec)
    assert (issue.code, issue.field) == (
        AgentErrorCode.USECASE_KEY_UNKNOWN,
        "capabilities.keys",
    )


class TestSql:
    def test_reports_connection_unknown_when_connection_is_not_configured(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
    ) -> None:
        spec = spec_factory(
            capabilities=(
                SqlCapability(connection="missing_conn", max_rows=10, max_result_bytes=1024),
            )
        )
        issue = single_issue_for(spec)
        assert (issue.code, issue.field) == (
            AgentErrorCode.SQL_CONNECTION_UNKNOWN,
            "capabilities.connection",
        )

    def test_reports_not_readonly_when_connection_permits_writes(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
    ) -> None:
        spec = spec_factory(
            capabilities=(SqlCapability(connection="writable", max_rows=10, max_result_bytes=1024),)
        )
        issue = single_issue_for(spec)
        assert (issue.code, issue.field) == (
            AgentErrorCode.SQL_CONNECTION_NOT_READONLY,
            "capabilities.connection",
        )

    def test_reports_config_missing_when_no_data_layer_config_is_supplied(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
    ) -> None:
        """FR-046a: absence of configuration must never skip the check silently."""
        issue = single_issue_for(spec_factory(capabilities=(_valid_sql(),)), sql=None)
        assert issue.code is AgentErrorCode.SQL_CONFIG_MISSING

    def test_reports_roles_unbound_when_allowlisted_connection_has_no_identity_to_bind(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        issues_for: Callable[..., tuple[AgentCompilationIssue, ...]],
        ai_config_factory: Callable[..., AiConfig],
    ) -> None:
        """FR-043a: without a verified identity the allowlist becomes a menu."""
        config = ai_config_factory(
            endpoints={
                "subject-agent": AgentEndpointConfig(
                    enabled=True, auth="bearer", allow_anonymous=True
                )
            }
        )
        spec = spec_factory(
            capabilities=(
                SqlCapability(connection="roles_menu", max_rows=10, max_result_bytes=1024),
            )
        )
        codes = {issue.code for issue in issues_for(spec, config=config)}
        assert AgentErrorCode.SQL_CONNECTION_ROLES_UNBOUND in codes


class TestSqlResultBoundMissing:
    """FR-046b, decision record.

    ``SqlCapability`` makes an unbounded query unrepresentable at Tier 1:
    ``max_rows`` and ``max_result_bytes`` are required fields constrained to
    ``>= 1`` at decode, so no ``AgentSpecV1`` can reach the compiler without
    bounds and the compiler phase has nothing left to reject. These tests pin
    that irrepresentability (the guarantee T061 relies on) and that the code
    stays published in the catalogue for external constructors of issues.
    """

    def test_decode_rejects_sql_capability_when_bounds_are_absent(self) -> None:
        with pytest.raises(msgspec.ValidationError):
            msgspec.convert(
                {"kind": "sql", "connection": "reporting_readonly"},
                type=CapabilitySpec,
            )

    @pytest.mark.parametrize("bound_field", ["max_rows", "max_result_bytes"])
    def test_decode_rejects_sql_capability_when_a_bound_is_not_positive(
        self, bound_field: str
    ) -> None:
        payload: dict[str, Any] = {
            "kind": "sql",
            "connection": "reporting_readonly",
            "max_rows": 10,
            "max_result_bytes": 1024,
            bound_field: 0,
        }
        with pytest.raises(msgspec.ValidationError):
            msgspec.convert(payload, type=CapabilitySpec)

    def test_error_code_stays_published_in_the_catalogue(self) -> None:
        assert AgentErrorCode.SQL_RESULT_BOUND_MISSING.value == "SQL_RESULT_BOUND_MISSING"


@pytest.mark.parametrize("url", _BAD_URLS, ids=_BAD_URL_IDS)
def test_reports_mcp_url_invalid_when_url_is_unsafe(
    spec_factory: Callable[..., AgentSpecV1],
    single_issue_for: Callable[..., AgentCompilationIssue],
    url: str,
) -> None:
    spec = spec_factory(capabilities=(McpCapability(url=url),))
    issue = single_issue_for(spec)
    assert (issue.code, issue.field) == (AgentErrorCode.MCP_URL_INVALID, "capabilities.url")


@pytest.mark.parametrize("url", _BAD_URLS, ids=_BAD_URL_IDS)
def test_reports_a2a_url_invalid_when_url_is_unsafe(
    spec_factory: Callable[..., AgentSpecV1],
    single_issue_for: Callable[..., AgentCompilationIssue],
    url: str,
) -> None:
    spec = spec_factory(capabilities=(A2ACapability(url=url),))
    issue = single_issue_for(spec)
    assert (issue.code, issue.field) == (AgentErrorCode.A2A_URL_INVALID, "capabilities.url")


class TestSkills:
    def test_reports_ref_invalid_when_skill_reference_does_not_import(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
    ) -> None:
        spec = spec_factory(
            capabilities=(SkillsCapability(refs=("no_such_pkg_zz.skills:helper",)),)
        )
        issue = single_issue_for(spec)
        assert (issue.code, issue.field) == (
            AgentErrorCode.SKILLS_REF_INVALID,
            "capabilities.refs",
        )

    def test_reports_root_missing_when_config_declares_no_skills_root(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
        ai_config_factory: Callable[..., AiConfig],
    ) -> None:
        spec = spec_factory(
            capabilities=(SkillsCapability(refs=("myapp.skills.writing:tone_of_voice",)),)
        )
        issue = single_issue_for(spec, config=ai_config_factory(skills_root=None))
        assert issue.code is AgentErrorCode.SKILLS_ROOT_MISSING


class TestPythonFactory:
    def test_reports_unresolvable_when_factory_does_not_import(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
    ) -> None:
        spec = spec_factory(capabilities=(PythonCapability(factory="no_such_pkg_zz.tools:build"),))
        issue = single_issue_for(spec)
        assert (issue.code, issue.field) == (
            AgentErrorCode.PYTHON_FACTORY_UNRESOLVABLE,
            "capabilities.factory",
        )

    def test_reports_not_callable_when_factory_resolves_to_a_non_callable(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
    ) -> None:
        spec = spec_factory(
            capabilities=(PythonCapability(factory="myapp.tools.broken:NOT_CALLABLE"),)
        )
        issue = single_issue_for(spec)
        assert (issue.code, issue.field) == (
            AgentErrorCode.PYTHON_FACTORY_NOT_CALLABLE,
            "capabilities.factory",
        )


class TestAnonymousOptOut:
    """FR-045a: anonymous agents may hold no data or remote capability.

    The opt-out is ``AiConfig.endpoints[name].allow_anonymous = True``.
    Data/remote kinds: ``usecase`` and ``sql`` read application data;
    ``python``, ``mcp`` and ``a2a`` run application code or call a remote
    system. ``skills`` only injects packaged prompt material, so it is the
    one kind an anonymous agent may keep.
    """

    @staticmethod
    def _anonymous_config(ai_config_factory: Callable[..., AiConfig]) -> AiConfig:
        return ai_config_factory(
            endpoints={
                "subject-agent": AgentEndpointConfig(
                    enabled=True, auth="bearer", allow_anonymous=True
                )
            }
        )

    @pytest.mark.parametrize(
        "capability",
        [
            UsecaseCapability(keys=("orders.get_order_status",)),
            _valid_sql(),
            PythonCapability(factory="myapp.tools.geo:build_geo_toolset"),
            McpCapability(url="https://tools.example.com/mcp"),
            A2ACapability(url="https://remote.example.com/a2a"),
        ],
        ids=["usecase", "sql", "python", "mcp", "a2a"],
    )
    def test_reports_anonymous_with_data_capability_when_kind_reads_data_or_calls_remote(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        issues_for: Callable[..., tuple[AgentCompilationIssue, ...]],
        ai_config_factory: Callable[..., AiConfig],
        capability: object,
    ) -> None:
        spec = spec_factory(capabilities=(capability,))
        config = self._anonymous_config(ai_config_factory)
        codes = {issue.code for issue in issues_for(spec, config=config)}
        assert AgentErrorCode.ANONYMOUS_WITH_DATA_CAPABILITY in codes

    def test_compiles_clean_when_anonymous_agent_holds_only_skills(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        compiler_factory: Callable[..., Any],
        ai_config_factory: Callable[..., AiConfig],
    ) -> None:
        spec = spec_factory(
            capabilities=(SkillsCapability(refs=("myapp.skills.writing:tone_of_voice",)),)
        )
        compiler = compiler_factory(config=self._anonymous_config(ai_config_factory))
        plan = compiler.compile(spec)
        assert plan.name == "subject-agent"
