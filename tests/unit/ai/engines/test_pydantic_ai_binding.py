"""``InferenceTarget`` → pydantic-ai model binding, and the spec translation.

No network: building a model object configures a client, it does not call one.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from loom.ai.compiler._plan import AgentPlan
from loom.ai.engines.pydantic_ai._models import SUPPORTED_PROVIDERS, resolve_model
from loom.ai.engines.pydantic_ai._spec import build_agent_spec
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.inference import InferenceTarget
from tests.helpers.pydantic_ai_engine import make_plan


@pytest.fixture
def aws_profile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    """A named AWS profile the Bedrock binding can actually resolve."""
    credentials = tmp_path / "credentials"
    credentials.write_text(
        "[reporting-profile]\naws_access_key_id = AKIAPROFILE\naws_secret_access_key = secret\n"
    )
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "config"))
    return "reporting-profile"


class TestModelBinding:
    def test_bedrock_lleva_region_y_credenciales_cuando_se_resuelve(self, aws_profile: str) -> None:
        """The Bedrock binding carries region and profile into the client."""
        target = InferenceTarget(
            provider="bedrock",
            model="anthropic.claude-sonnet-4-5-20250929-v1:0",
            region="eu-west-1",
            credentials_ref=aws_profile,
        )

        model = resolve_model(target)

        assert model.model_name == target.model
        assert model.client.meta.region_name == "eu-west-1"
        assert model.client._request_signer._credentials.access_key == "AKIAPROFILE"

    @pytest.mark.parametrize(
        ("provider", "model_id"),
        [("openai", "gpt-5.2"), ("anthropic", "claude-sonnet-4-5")],
    )
    def test_el_modelo_lleva_el_id_del_vendor_cuando_se_resuelve(
        self, provider: str, model_id: str
    ) -> None:
        """OpenAI and Anthropic bind the vendor model id unchanged."""
        target = InferenceTarget(provider=provider, model=model_id, credentials_ref="a-key")

        assert resolve_model(target).model_name == model_id

    def test_falla_nombrando_los_proveedores_cuando_el_vendor_es_desconocido(self) -> None:
        """An unknown provider dies at start-up, naming what this release binds."""
        with pytest.raises(AgentCompilationError) as failure:
            resolve_model(InferenceTarget(provider="unheard-of", model="x"))

        issue = failure.value.issues[0]
        assert issue.code is AgentErrorCode.PROVIDER_NOT_INSTALLED
        assert "openai" in issue.message

    def test_falla_pidiendo_la_region_cuando_bedrock_no_la_trae(self) -> None:
        """A Bedrock binding without a region fails before any request."""
        with pytest.raises(AgentCompilationError) as failure:
            resolve_model(InferenceTarget(provider="bedrock", model="x"))

        assert failure.value.issues[0].code is AgentErrorCode.PROVIDER_SETTING_MISSING

    def test_los_proveedores_soportados_son_los_documentados(self) -> None:
        """The dispatch map is the whole vendor surface of this release."""
        assert frozenset({"bedrock", "openai", "anthropic", "gateway"}) == SUPPORTED_PROVIDERS


class TestSpecTranslation:
    def test_el_spec_lleva_el_esquema_y_las_politicas_cuando_se_traduce(self) -> None:
        """The plan's output schema and limits reach the engine's own spec."""
        plan: AgentPlan = make_plan(retries=3)

        spec = build_agent_spec(plan)

        assert spec.output_schema == dict(plan.output.schema)
        assert spec.retries == 3
        assert spec.tool_timeout == plan.policies.tool_timeout_ms / 1000

    def test_el_spec_no_lleva_metadata_ni_modelo_cuando_se_traduce(self) -> None:
        """Ownership facts and the concrete model never travel in the spec."""
        spec = build_agent_spec(make_plan())

        assert spec.metadata is None
        assert spec.model is None


class TestEntryPoint:
    def test_el_motor_se_resuelve_por_entry_point_cuando_se_pide_por_nombre(self) -> None:
        """``ai.engine: pydantic-ai`` resolves, handshake included (FR-021)."""
        from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider
        from loom.ai.registry import resolve_engine_provider

        provider = resolve_engine_provider("pydantic-ai")

        assert isinstance(provider, PydanticAIEngineProvider)
        assert PydanticAIEngineProvider.LOOM_AI_ENGINE_API == 1

    # The kinds this adapter declares are asserted by
    # ``tests/integration/ai/test_capabilities.py::TestSupportedCapabilityKinds``,
    # which supersedes the empty-set assertion that stood here while the
    # capability toolsets did not exist yet.
