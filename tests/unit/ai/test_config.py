"""Deployment configuration contract for ``loom.ai.config`` (T031).

Pins the Tier-2 validation rules of the data model: provider-incomplete model
bindings, literal secrets in ``credentials_ref``, empty A2A exposure, the
mandatory per-endpoint ``auth``, and the documented ``AiConfig`` defaults.

Since artifacts *name* remote servers and agents instead of locating them, this
is the only layer where a URL, a credential reference or a per-call deadline
exists — so it is also the only layer where they can be rejected.

Assertions are made on error *codes* (``AgentErrorCode``), never on wording,
except where the contract is precisely about the message (secret absence).
"""

from __future__ import annotations

from typing import get_args

import msgspec
import pytest

from loom.ai.config import (
    A2AAgentConfig,
    A2AConfig,
    AgentEndpointConfig,
    AiConfig,
    McpServerConfig,
)
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.inference import OUTPUT_MODES, InferenceTarget, OutputMode


def _codes(error: AgentCompilationError) -> list[AgentErrorCode]:
    """Extract the ordered issue codes carried by a compilation error."""
    return [issue.code for issue in error.issues]


def _complete_target() -> InferenceTarget:
    """Build a binding complete for its provider."""
    return InferenceTarget(provider="openai", model="gpt-test")


def _config(models: dict[str, InferenceTarget]) -> AiConfig:
    """Build a minimal ``AiConfig`` around the given model bindings."""
    return AiConfig(
        engine="pydantic-ai",
        specs=("ai/agents/*/agent.yaml",),
        models=models,
    )


def _config_with(**overrides: object) -> AiConfig:
    """Build a valid ``AiConfig`` with the registry under test substituted in."""
    return AiConfig(
        engine="pydantic-ai",
        specs=("ai/agents/*/agent.yaml",),
        models={"default": _complete_target()},
        **overrides,  # type: ignore[arg-type]
    )


_UNSAFE_URLS: list[str] = [
    "http://knowledge.example.com/mcp",
    "https://user:pass@knowledge.example.com/mcp",
    "https://knowledge.example.com/mcp?token=abc",
    "not a url",
    "https:///mcp",
]
_UNSAFE_URL_IDS: list[str] = [
    "plain_http",
    "userinfo",
    "query_string",
    "malformed",
    "no_host",
]


class TestModelBindingValidation:
    def test_falla_con_inference_target_incomplete_cuando_bedrock_no_declara_region(
        self,
    ) -> None:
        """A bedrock binding without ``region`` is unusable and must be rejected."""
        incomplete = InferenceTarget(provider="bedrock", model="anthropic.claude-v1")

        with pytest.raises(AgentCompilationError) as excinfo:
            _config({"default": incomplete})

        assert AgentErrorCode.INFERENCE_TARGET_INCOMPLETE in _codes(excinfo.value)

    @pytest.mark.parametrize("mode", ["prompted", "xml"])
    def test_falla_con_output_mode_unknown_cuando_el_modo_no_es_tool_ni_native(
        self, mode: str
    ) -> None:
        """Only ``tool`` and ``native`` are offered; the issue names the role and both."""
        target = InferenceTarget(provider="openai", model="gpt-test", output_mode=mode)

        with pytest.raises(AgentCompilationError) as excinfo:
            _config({"reporting": target})

        issue = next(
            issue
            for issue in excinfo.value.issues
            if issue.code is AgentErrorCode.OUTPUT_MODE_UNKNOWN
        )
        assert "reporting" in issue.message
        assert "tool, native" in issue.message

    @pytest.mark.parametrize("mode", ["tool", "native", None])
    def test_acepta_el_binding_cuando_output_mode_es_valido_o_no_se_declara(
        self, mode: str | None
    ) -> None:
        """A declared valid mode, or none at all, loads without issues."""
        target = InferenceTarget(provider="openai", model="gpt-test", output_mode=mode)

        config = _config({"default": target})

        assert config.models["default"].output_mode == mode

    def test_falla_con_output_mode_unknown_cuando_el_modo_llega_por_yaml(self) -> None:
        """The decode path reports the loom issue, not a raw msgspec error.

        The struct field is deliberately ``str``: a ``Literal`` would make
        msgspec refuse the value during the decode, before ``__post_init__``
        could name the role in an ``OUTPUT_MODE_UNKNOWN`` issue.
        """
        document = b"""
engine: pydantic-ai
specs: ["ai/agents/*/agent.yaml"]
models:
  reporting:
    provider: openai
    model: gpt-test
    output_mode: prompted
"""

        with pytest.raises(AgentCompilationError) as excinfo:
            msgspec.yaml.decode(document, type=AiConfig)

        issue = next(
            issue
            for issue in excinfo.value.issues
            if issue.code is AgentErrorCode.OUTPUT_MODE_UNKNOWN
        )
        assert "reporting" in issue.message
        assert "tool, native" in issue.message

    def test_las_constantes_de_modo_derivan_del_tipo(self) -> None:
        """``OUTPUT_MODES`` and ``OutputMode`` cannot drift: one derives from the other."""
        assert get_args(OutputMode) == OUTPUT_MODES

    def test_el_repr_muestra_output_mode_cuando_se_declara(self) -> None:
        """``output_mode`` is not a secret, so the redacting repr shows it."""
        target = InferenceTarget(provider="openai", model="gpt-test", output_mode="native")

        assert "output_mode='native'" in repr(target)


class TestLiteralSecretRejection:
    @pytest.mark.parametrize(
        "literal_secret",
        [
            "AKIAIOSFODNN7EXAMPLE",
            "sk-abc123def456ghi789",
            "this value has spaces",
            "https://user:pass@vault.example.com/creds",
        ],
        ids=["aws_access_key", "sk_token", "with_spaces", "url_userinfo"],
    )
    def test_falla_cuando_credentials_ref_es_un_secreto_literal(
        self,
        literal_secret: str,
    ) -> None:
        """A literal secret in ``credentials_ref`` is never accepted (FR-018)."""
        target = InferenceTarget(
            provider="openai",
            model="gpt-test",
            credentials_ref=literal_secret,
        )

        with pytest.raises(AgentCompilationError):
            _config({"default": target})

    @pytest.mark.parametrize(
        "literal_secret",
        [
            "AKIAIOSFODNN7EXAMPLE",
            "sk-abc123def456ghi789",
            "this value has spaces",
            "https://user:pass@vault.example.com/creds",
        ],
        ids=["aws_access_key", "sk_token", "with_spaces", "url_userinfo"],
    )
    def test_el_mensaje_no_contiene_el_secreto_cuando_se_rechaza_un_literal(
        self,
        literal_secret: str,
    ) -> None:
        """The rejection itself must not leak the full secret it rejects."""
        target = InferenceTarget(
            provider="openai",
            model="gpt-test",
            credentials_ref=literal_secret,
        )

        with pytest.raises(AgentCompilationError) as excinfo:
            _config({"default": target})

        assert literal_secret not in str(excinfo.value)


class TestA2AConfig:
    def test_falla_con_a2a_expose_empty_cuando_expose_esta_vacio(self) -> None:
        """Empty ``expose`` means none, never all (FR-041a); it must be rejected."""
        with pytest.raises(AgentCompilationError) as excinfo:
            A2AConfig(base_url="https://agents.example.com", expose=())

        assert AgentErrorCode.A2A_EXPOSE_EMPTY in _codes(excinfo.value)


class TestAgentEndpointConfig:
    def test_falla_la_construccion_cuando_no_se_declara_auth(self) -> None:
        """``auth`` has no default: omitting it must fail at construction."""
        with pytest.raises(TypeError):
            AgentEndpointConfig(enabled=True)  # type: ignore[call-arg]

    def test_allow_anonymous_es_false_cuando_no_se_declara(self) -> None:
        """Anonymous access is opt-in per agent (FR-045a)."""
        endpoint = AgentEndpointConfig(enabled=True, auth="oidc")

        assert endpoint.allow_anonymous is False


class TestAiConfigDefaults:
    @pytest.fixture()
    def config(self) -> AiConfig:
        """Build a minimal valid config relying on every default."""
        return _config({"default": _complete_target()})

    def test_startup_timeout_ms_por_defecto_cuando_no_se_declara(
        self,
        config: AiConfig,
    ) -> None:
        assert config.startup_timeout_ms == 10000

    def test_max_concurrent_runs_por_defecto_cuando_no_se_declara(
        self,
        config: AiConfig,
    ) -> None:
        assert config.max_concurrent_runs == 8

    def test_max_prompt_bytes_por_defecto_cuando_no_se_declara(
        self,
        config: AiConfig,
    ) -> None:
        assert config.max_prompt_bytes == 65536

    def test_health_cache_ttl_ms_por_defecto_cuando_no_se_declara(
        self,
        config: AiConfig,
    ) -> None:
        assert config.health_cache_ttl_ms == 5000

    def test_a2a_es_none_cuando_no_se_declara(self, config: AiConfig) -> None:
        """Absent ``a2a`` means no card and no A2A endpoints (FR-041)."""
        assert config.a2a is None

    def test_endpoints_esta_vacio_cuando_no_se_declara(self, config: AiConfig) -> None:
        """An agent absent from ``endpoints`` is never mounted (FR-029a)."""
        assert dict(config.endpoints) == {}


class TestMcpServerRegistry:
    """``ai.mcp_servers`` is where an artifact's ``server:`` name is located."""

    @pytest.mark.parametrize("url", _UNSAFE_URLS, ids=_UNSAFE_URL_IDS)
    def test_falla_con_mcp_url_invalid_cuando_la_url_no_es_segura(self, url: str) -> None:
        """Only a plain ``https`` URL without userinfo or query string is accepted."""
        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(mcp_servers={"knowledge": McpServerConfig(url=url)})

        assert AgentErrorCode.MCP_URL_INVALID in _codes(excinfo.value)

    def test_el_mensaje_no_contiene_el_userinfo_cuando_se_rechaza_la_url(self) -> None:
        """The rejection must not leak the credential embedded in the URL."""
        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(
                mcp_servers={
                    "knowledge": McpServerConfig(
                        url="https://user:s3cr3t@knowledge.example.com/mcp"
                    )
                }
            )

        assert "s3cr3t" not in str(excinfo.value)

    def test_falla_con_credentials_inline_cuando_headers_ref_es_un_secreto_literal(self) -> None:
        """``headers_ref`` references headers; it never carries them (FR-018)."""
        server = McpServerConfig(
            url="https://knowledge.example.com/mcp",
            headers_ref="sk-abc123def456ghi789",
        )

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(mcp_servers={"knowledge": server})

        assert AgentErrorCode.MCP_CREDENTIALS_INLINE in _codes(excinfo.value)

    def test_el_mensaje_no_contiene_el_secreto_cuando_se_rechaza_headers_ref(self) -> None:
        """The rejection itself must not leak the very secret it rejects."""
        literal = "sk-abc123def456ghi789"
        server = McpServerConfig(url="https://knowledge.example.com/mcp", headers_ref=literal)

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(mcp_servers={"knowledge": server})

        assert literal not in str(excinfo.value)

    @pytest.mark.parametrize("timeout_ms", [0, -1, 600001], ids=["zero", "negative", "above_max"])
    def test_falla_con_policy_out_of_range_cuando_el_timeout_esta_fuera_de_rango(
        self,
        timeout_ms: int,
    ) -> None:
        """A per-call deadline outside ``1..600000`` is a configuration fault."""
        server = McpServerConfig(url="https://knowledge.example.com/mcp", timeout_ms=timeout_ms)

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(mcp_servers={"knowledge": server})

        assert AgentErrorCode.POLICY_OUT_OF_RANGE in _codes(excinfo.value)

    def test_acepta_el_servidor_cuando_la_url_y_la_referencia_son_validas(self) -> None:
        """The happy case: an https URL, a reference-shaped secret and a sane deadline."""
        server = McpServerConfig(
            url="https://knowledge.example.com/mcp",
            headers_ref="ai/knowledge/headers",
            timeout_ms=15000,
        )

        config = _config_with(mcp_servers={"knowledge": server})

        assert config.mcp_servers["knowledge"] == server

    def test_timeout_ms_por_defecto_cuando_no_se_declara(self) -> None:
        """The documented per-call deadline applies when the deployment stays silent."""
        assert McpServerConfig(url="https://knowledge.example.com/mcp").timeout_ms == 20000

    def test_mcp_servers_esta_vacio_cuando_no_se_declara(self) -> None:
        """No server is reachable by default; every name must be declared."""
        assert dict(_config_with().mcp_servers) == {}


class TestA2AAgentRegistry:
    """``ai.a2a_agents`` is where an artifact's ``agent:`` name is located."""

    @pytest.mark.parametrize("url", _UNSAFE_URLS, ids=_UNSAFE_URL_IDS)
    def test_falla_con_a2a_url_invalid_cuando_la_url_no_es_segura(self, url: str) -> None:
        """Remote agents are held to the same URL rules as remote tool servers."""
        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(a2a_agents={"translations": A2AAgentConfig(url=url)})

        assert AgentErrorCode.A2A_URL_INVALID in _codes(excinfo.value)

    def test_falla_con_credentials_inline_cuando_headers_ref_es_un_secreto_literal(self) -> None:
        """``headers_ref`` references headers; it never carries them (FR-018)."""
        agent = A2AAgentConfig(
            url="https://translations.example.com/a2a",
            headers_ref="AKIAIOSFODNN7EXAMPLE",
        )

        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(a2a_agents={"translations": agent})

        assert AgentErrorCode.MCP_CREDENTIALS_INLINE in _codes(excinfo.value)

    def test_acepta_el_agente_cuando_la_url_y_la_referencia_son_validas(self) -> None:
        """The happy case: an https URL and a reference-shaped secret."""
        agent = A2AAgentConfig(
            url="https://translations.example.com/a2a",
            headers_ref="ai/translations/headers",
        )

        config = _config_with(a2a_agents={"translations": agent})

        assert config.a2a_agents["translations"] == agent

    def test_a2a_agents_esta_vacio_cuando_no_se_declara(self) -> None:
        """No remote agent is reachable by default; every name must be declared."""
        assert dict(_config_with().a2a_agents) == {}


class TestSkillsRoot:
    def test_skills_root_es_none_cuando_no_se_declara(self) -> None:
        """A bare library name needs a root; without one the compiler must complain."""
        assert _config_with().skills_root is None

    def test_conserva_el_skills_root_cuando_se_declara(self) -> None:
        """The root is deployment-owned; the artifact never carries a path."""
        assert _config_with(skills_root="/srv/app/skills").skills_root == "/srv/app/skills"


class TestAggregatedRegistryIssues:
    def test_acumula_una_incidencia_por_registro_invalido_cuando_ambos_fallan(self) -> None:
        """One raise reports every faulty entry, never the first one only (FR-011)."""
        with pytest.raises(AgentCompilationError) as excinfo:
            _config_with(
                mcp_servers={"knowledge": McpServerConfig(url="http://knowledge.example.com/mcp")},
                a2a_agents={
                    "translations": A2AAgentConfig(url="http://translations.example.com/a2a")
                },
            )

        assert set(_codes(excinfo.value)) == {
            AgentErrorCode.MCP_URL_INVALID,
            AgentErrorCode.A2A_URL_INVALID,
        }
