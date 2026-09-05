"""``InferenceTarget`` → pydantic-ai model binding, and the spec translation.

No network: building a model object configures a client, it does not call one.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from msgspec import structs
from pydantic import TypeAdapter
from pydantic_ai import NativeOutput, ToolOutput
from pydantic_ai.models.bedrock import BedrockConverseModel
from pydantic_ai.models.test import TestModel

from loom.ai.compiler._plan import AgentPlan
from loom.ai.engines.pydantic_ai._models import SUPPORTED_PROVIDERS, resolve_model
from loom.ai.engines.pydantic_ai._spec import build_agent_spec, build_output_type
from loom.ai.engines.pydantic_ai.provider import PydanticAIEngineProvider
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.inference import InferenceTarget
from loom.core.di import LoomContainer
from tests.helpers.pydantic_ai_engine import STRICT_SCHEMA, NullDeps, make_plan


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

        assert isinstance(model, BedrockConverseModel)
        assert model.model_name == target.model
        assert model.client.meta.region_name == "eu-west-1"
        assert model.client._request_signer._credentials.access_key == "AKIAPROFILE"

    @pytest.mark.parametrize(
        ("provider", "model_id"),
        [("openai", "gpt-5.2"), ("anthropic", "claude-sonnet-4-5")],
    )
    def test_el_modelo_lleva_el_id_del_vendor_cuando_se_resuelve(
        self, provider: str, model_id: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """OpenAI and Anthropic bind the vendor model id unchanged."""
        monkeypatch.setenv("VENDOR_API_KEY", "a-key")
        target = InferenceTarget(
            provider=provider, model=model_id, credentials_ref="VENDOR_API_KEY"
        )

        assert resolve_model(target).model_name == model_id

    @pytest.mark.parametrize("provider", ["openai", "anthropic"])
    def test_lee_la_clave_de_la_variable_que_nombra_credentials_ref(
        self, provider: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``credentials_ref`` names the variable holding the key, never the key."""
        monkeypatch.setenv("VENDOR_API_KEY", "the-real-key")
        target = InferenceTarget(
            provider=provider, model="a-model", credentials_ref="VENDOR_API_KEY"
        )

        model = resolve_model(target)

        assert model.client.api_key == "the-real-key"

    def test_lee_la_variable_por_defecto_del_sdk_cuando_no_hay_credentials_ref(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Without a reference the vendor SDK reads its own variable."""
        monkeypatch.setenv("OPENAI_API_KEY", "sdk-default")
        target = InferenceTarget(provider="openai", model="a-model")

        assert resolve_model(target).client.api_key == "sdk-default"

    @pytest.mark.parametrize("provider", ["openai", "anthropic", "gateway"])
    def test_falla_nombrando_la_variable_cuando_no_esta_puesta(
        self, provider: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An unset variable is a start-up failure, not a 401 on the first call."""
        monkeypatch.delenv("VENDOR_API_KEY", raising=False)
        target = InferenceTarget(
            provider=provider,
            model="a-model",
            endpoint="https://gateway.example.com/v1" if provider == "gateway" else None,
            credentials_ref="VENDOR_API_KEY",
        )

        with pytest.raises(AgentCompilationError) as failure:
            resolve_model(target)

        issue = failure.value.issues[0]
        assert issue.code is AgentErrorCode.PROVIDER_SETTING_MISSING
        assert "VENDOR_API_KEY" in issue.message
        assert provider in issue.message

    def test_falla_nombrando_los_proveedores_cuando_el_vendor_es_desconocido(self) -> None:
        """An unknown provider dies at start-up, naming what this release binds."""
        with pytest.raises(AgentCompilationError) as failure:
            resolve_model(InferenceTarget(provider="unheard-of", model="x"))

        issue = failure.value.issues[0]
        assert issue.code is AgentErrorCode.PROVIDER_UNKNOWN
        assert "openai" in issue.message
        assert "extra" not in issue.message
        assert "not installed" not in issue.message

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

    def test_el_spec_no_lleva_tool_timeout_cuando_loom_ya_lo_aplica(self) -> None:
        """``tool_timeout_ms`` has one enforcer, so the engine gets no deadline.

        Projecting it here as well raced loom's own ``asyncio.timeout`` over
        the same value: the engine's expiry classifies as
        ``PROVIDER_UNAVAILABLE`` and is retried, loom's raises ``TOOL_TIMEOUT``
        and is not. Which one fired was up to the event loop.
        """
        spec = build_agent_spec(make_plan())

        assert spec.tool_timeout is None

    def test_el_spec_no_lleva_metadata_ni_modelo_cuando_se_traduce(self) -> None:
        """Ownership facts and the concrete model never travel in the spec."""
        spec = build_agent_spec(make_plan())

        assert spec.metadata is None
        assert spec.model is None


def _plan_with_output_mode(mode: str | None) -> AgentPlan:
    """A plan whose binding pins ``mode`` (or leaves it to the engine)."""
    plan = make_plan(schema=STRICT_SCHEMA)
    return structs.replace(plan, inference=structs.replace(plan.inference, output_mode=mode))


def _wrapped_schema(wrapped: Any) -> dict[str, Any]:
    """The schema pydantic derives from the marker's payload, sans decoration."""
    schema = TypeAdapter(wrapped).json_schema()
    schema.pop("title", None)
    schema.pop("description", None)
    return schema


class _FromSpecRecorder:
    """Stands in for ``Agent.from_spec`` and keeps the keywords it received.

    Also keeps the marker the provider built, because ``StructuredDict`` mints
    a new class per call: only identity proves the provider passed on the
    very object :func:`build_output_type` returned.
    """

    def __init__(self) -> None:
        self.kwargs: dict[str, Any] | None = None
        self.built: ToolOutput[Any] | NativeOutput[Any] | None = None

    def __call__(self, spec: object, **kwargs: Any) -> object:
        self.kwargs = kwargs
        return object()

    def build(self, plan: AgentPlan) -> ToolOutput[Any] | NativeOutput[Any] | None:
        self.built = build_output_type(plan)
        return self.built


@pytest.fixture
def from_spec(monkeypatch: pytest.MonkeyPatch) -> _FromSpecRecorder:
    """Patch ``Agent.from_spec`` and record the provider's marker, in its module."""
    recorder = _FromSpecRecorder()
    monkeypatch.setattr("loom.ai.engines.pydantic_ai.provider.Agent.from_spec", recorder)
    monkeypatch.setattr("loom.ai.engines.pydantic_ai.provider.build_output_type", recorder.build)
    return recorder


def _create_engine(plan: AgentPlan) -> None:
    provider = PydanticAIEngineProvider(model_resolver=lambda target: TestModel())
    provider.create_engine(plan, deps=NullDeps(), container=LoomContainer())


class TestOutputMode:
    def test_tool_envuelve_el_esquema_en_tool_output_y_llega_a_from_spec(
        self, from_spec: _FromSpecRecorder
    ) -> None:
        """``tool`` pins the tool-call mode around the plan's own schema."""
        plan = _plan_with_output_mode("tool")

        _create_engine(plan)

        marker = from_spec.built
        assert marker is not None
        assert type(marker) is ToolOutput
        assert _wrapped_schema(marker.output) == dict(plan.output.schema)
        assert from_spec.kwargs is not None
        assert from_spec.kwargs["output_type"] is marker

    def test_native_envuelve_el_esquema_en_native_output_y_llega_a_from_spec(
        self, from_spec: _FromSpecRecorder
    ) -> None:
        """``native`` pins the provider's structured-output mode, one type only."""
        plan = _plan_with_output_mode("native")

        _create_engine(plan)

        marker = from_spec.built
        assert marker is not None
        assert type(marker) is NativeOutput
        assert not isinstance(marker.outputs, list | tuple)
        assert _wrapped_schema(marker.outputs) == dict(plan.output.schema)
        assert from_spec.kwargs is not None
        assert from_spec.kwargs["output_type"] is marker

    def test_no_pasa_output_type_cuando_el_binding_no_declara_modo(
        self, from_spec: _FromSpecRecorder
    ) -> None:
        """Absent mode: the call is today's call, no ``output_type`` keyword at all."""
        plan = _plan_with_output_mode(None)

        assert build_output_type(plan) is None
        _create_engine(plan)

        assert from_spec.kwargs is not None
        assert "output_type" not in from_spec.kwargs

    def test_falla_cuando_el_modo_no_es_ninguno_de_los_dos(self) -> None:
        """Fail closed: an unhandled mode raises instead of degrading to ``native``.

        Reachable only past the config check (a plan built in process), which
        is exactly the path the defaulted dispatch used to serve silently.
        """
        plan = _plan_with_output_mode("prompted")

        with pytest.raises(AssertionError):
            build_output_type(plan)

    def test_el_marcador_no_lleva_nombre_ni_descripcion(self) -> None:
        """Mirrors the engine's own wrapping of ``output_schema``: bare marker."""
        tool = build_output_type(_plan_with_output_mode("tool"))
        native = build_output_type(_plan_with_output_mode("native"))

        assert tool is not None
        assert native is not None
        assert isinstance(tool, ToolOutput)
        assert isinstance(native, NativeOutput)
        assert (tool.name, tool.description) == (None, None)
        assert (native.name, native.description) == (None, None)


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
