"""``InferenceTarget`` → pydantic-ai model and provider objects (R-002).

One model object is built per plan, at start-up, and reused by every run: the
provider client it carries owns the connection pool, so rebuilding it per
request would pay a new TLS handshake for every prompt.

The provider dispatch is a mapping from ``InferenceTarget.provider`` to a
builder, not a chain of ``if`` branches: adding a vendor is one entry, and an
unknown vendor fails at start-up naming the ones this release binds.

Credential conventions, deliberately explicit (FR-018):

* ``bedrock`` — ``credentials_ref`` is an **AWS profile name**; when absent the
  standard boto3 chain (environment, role, instance profile) applies.
* ``openai``, ``anthropic``, ``gateway`` — ``credentials_ref`` is the API key
  the deployment's secret resolver already resolved.

``options`` is handed to the model as pydantic-ai ``ModelSettings`` — the
engine's own vendor-settings vocabulary — so loom introduces no second
settings dialect of its own.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import Protocol, cast

from pydantic_ai.models import Model
from pydantic_ai.settings import ModelSettings

from loom.ai.errors import AgentCompilationError, provider_not_installed
from loom.ai.inference import InferenceTarget
from loom.ai.registry import require_provider_sdk, require_provider_setting


class ModelResolver(Protocol):
    """Builds the engine model that serves one resolved binding.

    The provider takes one as an optional argument so a deployment can supply
    a preconfigured client — and so the shared engine contract suite can run
    this adapter against a scripted model with no network and no credentials
    (FR-048). Production never passes one: the default resolver below is used.
    """

    def __call__(self, target: InferenceTarget) -> Model:
        """Build the model object for ``target``.

        Args:
            target: Resolved model binding carried by the plan.

        Returns:
            The engine model to run with.
        """
        ...


def _model_settings(target: InferenceTarget) -> ModelSettings | None:
    """Vendor settings as the engine's own ``ModelSettings`` mapping."""
    if not target.options:
        return None
    # ``ModelSettings`` is a total=False TypedDict; the values come from
    # deployment configuration and cannot be named statically.
    return cast(ModelSettings, dict(target.options))


def _bedrock_model(target: InferenceTarget) -> Model:
    require_provider_setting("bedrock", "region", target.region)
    require_provider_sdk("bedrock", "pydantic_ai.models.bedrock", "ai-bedrock")
    from pydantic_ai.models.bedrock import BedrockConverseModel
    from pydantic_ai.providers.bedrock import BedrockProvider

    provider = BedrockProvider(
        region_name=target.region,
        profile_name=target.credentials_ref,
        base_url=target.endpoint,
    )
    return BedrockConverseModel(target.model, provider=provider, settings=_model_settings(target))


def _openai_model(target: InferenceTarget) -> Model:
    require_provider_sdk("openai", "pydantic_ai.models.openai", "ai-openai")
    from pydantic_ai.models.openai import OpenAIChatModel
    from pydantic_ai.providers.openai import OpenAIProvider

    provider = OpenAIProvider(base_url=target.endpoint, api_key=target.credentials_ref)
    return OpenAIChatModel(target.model, provider=provider, settings=_model_settings(target))


def _anthropic_model(target: InferenceTarget) -> Model:
    require_provider_sdk("anthropic", "pydantic_ai.models.anthropic", "ai-anthropic")
    from pydantic_ai.models.anthropic import AnthropicModel
    from pydantic_ai.providers.anthropic import AnthropicProvider

    provider = AnthropicProvider(api_key=target.credentials_ref, base_url=target.endpoint)
    return AnthropicModel(target.model, provider=provider, settings=_model_settings(target))


def _gateway_model(target: InferenceTarget) -> Model:
    """OpenAI-compatible gateway; its ``endpoint`` is required by config."""
    require_provider_setting("gateway", "endpoint", target.endpoint)
    return _openai_model(target)


_BUILDERS: Mapping[str, Callable[[InferenceTarget], Model]] = MappingProxyType(
    {
        "bedrock": _bedrock_model,
        "openai": _openai_model,
        "anthropic": _anthropic_model,
        "gateway": _gateway_model,
    }
)

SUPPORTED_PROVIDERS: frozenset[str] = frozenset(_BUILDERS)
"""Provider identifiers this release binds to a pydantic-ai model."""


def resolve_model(target: InferenceTarget) -> Model:
    """Build the pydantic-ai model bound to one resolved target.

    Args:
        target: Resolved model binding carried by the plan.

    Returns:
        The engine model, with its provider client already configured.

    Raises:
        AgentCompilationError: With ``PROVIDER_NOT_INSTALLED`` when the vendor
            SDK is missing (naming the extra) or the provider is unknown to
            this release, and with ``PROVIDER_SETTING_MISSING`` when a setting
            the vendor requires is absent.
    """
    builder = _BUILDERS.get(target.provider)
    if builder is None:
        known = ", ".join(sorted(SUPPORTED_PROVIDERS))
        raise AgentCompilationError(
            [provider_not_installed(target.provider, f"one of the providers: {known}")]
        )
    return builder(target)
