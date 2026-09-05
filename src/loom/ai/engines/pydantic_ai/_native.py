"""Provider-run tools: the loom name of each, and which binding admits it.

The mapping is by class, never by the engine's own identifier, so a rename in
pydantic-ai surfaces as a failed import here instead of a silently unsupported
grant.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final

from loom.ai.compiler._plan import CompiledNativeCapability
from loom.ai.errors import AgentCompilationError, provider_not_installed

if TYPE_CHECKING:
    from collections.abc import Mapping

    from loom.ai.compiler import AgentPlan
    from loom.ai.inference import InferenceTarget


def _tool_classes() -> Mapping[str, type[Any]]:
    """Return the loom tool name of every provider tool this release grants.

    Raises:
        AgentCompilationError: When the engine SDK is not installed.
    """
    try:
        from pydantic_ai.native_tools import CodeExecutionTool, WebFetchTool, WebSearchTool
    except ImportError as exc:  # pragma: no cover - the SDK is a hard dependency here
        raise AgentCompilationError([provider_not_installed("native", "ai-pydantic")]) from exc
    return MappingProxyType(
        {
            "web_search": WebSearchTool,
            "web_fetch": WebFetchTool,
            "code_execution": CodeExecutionTool,
        }
    )


NATIVE_TOOL_NAMES: Final[tuple[str, ...]] = ("web_search", "web_fetch", "code_execution")
"""Tool names this engine maps, in the order the artifact schema lists them."""


def supported_native_tools(target: InferenceTarget) -> frozenset[str]:
    """Return the tool names the model class bound to *target* admits.

    Answered from the class the provider binds, so no model is instantiated and
    no credential is read. The class is the upper bound: pydantic-ai narrows it
    again per model name when the request is made.

    Raises:
        AgentCompilationError: When the provider is unknown or its SDK missing.
    """
    from loom.ai.engines.pydantic_ai._models import model_class_for

    admitted = model_class_for(target).supported_native_tools()
    classes = _tool_classes()
    return frozenset(name for name, cls in classes.items() if cls in admitted)


def build_native_capabilities(plan: AgentPlan) -> tuple[Any, ...]:
    """Build one engine capability per granted provider tool, in plan order.

    Raises:
        AgentCompilationError: When the engine SDK is not installed.
    """
    grants = [c for c in plan.capabilities if isinstance(c, CompiledNativeCapability)]
    if not grants:
        return ()
    try:
        from pydantic_ai.capabilities import NativeTool
    except ImportError as exc:  # pragma: no cover - the SDK is a hard dependency here
        raise AgentCompilationError([provider_not_installed("native", "ai-pydantic")]) from exc
    classes = _tool_classes()
    return tuple(NativeTool(classes[grant.tool]()) for grant in grants)
