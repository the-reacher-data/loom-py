"""Any OpenAI-compatible endpoint: the OpenAI island with ``endpoint`` required."""

from __future__ import annotations

from pydantic_ai.models import Model

from loom.ai.engines.pydantic_ai.providers import openai
from loom.ai.inference import InferenceTarget
from loom.ai.registry import require_provider_setting

MODEL_CLASS: type[Model] = openai.MODEL_CLASS


def build(target: InferenceTarget) -> Model:
    """Build the OpenAI-compatible model of one binding; ``endpoint`` is required."""
    require_provider_setting("gateway", "endpoint", target.endpoint)
    return openai.build(target)
