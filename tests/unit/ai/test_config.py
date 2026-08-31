"""Deployment configuration contract for ``loom.ai.config`` (T031).

Pins the Tier-2 validation rules of the data model: provider-incomplete model
bindings, literal secrets in ``credentials_ref``, empty A2A exposure, the
mandatory per-endpoint ``auth``, and the documented ``AiConfig`` defaults.

Assertions are made on error *codes* (``AgentErrorCode``), never on wording,
except where the contract is precisely about the message (secret absence).
"""

from __future__ import annotations

import pytest

from loom.ai.config import A2AConfig, AgentEndpointConfig, AiConfig
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.inference import InferenceTarget


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
        specs=("agents/*.agent.yaml",),
        models=models,
    )


class TestModelBindingValidation:
    def test_falla_con_inference_target_incomplete_cuando_bedrock_no_declara_region(
        self,
    ) -> None:
        """A bedrock binding without ``region`` is unusable and must be rejected."""
        incomplete = InferenceTarget(provider="bedrock", model="anthropic.claude-v1")

        with pytest.raises(AgentCompilationError) as excinfo:
            _config({"default": incomplete})

        assert AgentErrorCode.INFERENCE_TARGET_INCOMPLETE in _codes(excinfo.value)


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
