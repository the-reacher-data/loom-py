"""Entry-point target of the pydantic-ai engine (group ``loom.ai.engines``).

The provider is what the ``ai.engine: pydantic-ai`` setting resolves to. It
builds one engine per plan — called exactly once per plan by
:class:`~loom.ai.runtime.AgentRuntime`, in ``__aenter__``, never per request —
and answers which capability kinds this adapter can serve.

The plan's output schema reaches the engine through the spec; a pinned
``output_mode`` on the model binding reaches it as ``output_type=`` on
``Agent.from_spec`` (see :func:`~loom.ai.engines.pydantic_ai._spec.build_output_type`),
and that keyword is absent when no mode is pinned.
"""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic_ai import Agent

from loom.ai.abc import AgentEngine, DepsFactory
from loom.ai.compiler import AgentPlan
from loom.ai.engines.pydantic_ai._a2a import create_a2a_client
from loom.ai.engines.pydantic_ai._capabilities import (
    SUPPORTED_KINDS,
    build_capabilities,
    build_toolsets,
)
from loom.ai.engines.pydantic_ai._engine import PydanticAIEngine
from loom.ai.engines.pydantic_ai._mcp import create_mcp_client
from loom.ai.engines.pydantic_ai._models import ModelResolver, resolve_model
from loom.ai.engines.pydantic_ai._native import supported_native_tools
from loom.ai.engines.pydantic_ai._spec import build_agent_spec, build_output_type
from loom.ai.inference import InferenceTarget
from loom.core.di import LoomContainer


class PydanticAIEngineProvider:
    """Builds pydantic-ai engines from compiled plans.

    Args:
        model_resolver: Builds the engine model from the plan's resolved
            binding. Defaults to the provider bindings of
            :mod:`~loom.ai.engines.pydantic_ai._models`; a deployment supplies
            one to inject a preconfigured vendor client, and the shared engine
            contract suite supplies one to exercise this adapter with no
            network and no credentials (FR-048).

    Attributes:
        LOOM_AI_ENGINE_API: Handshake version, read with ``getattr`` on load.

    Example::

        provider = PydanticAIEngineProvider()
        engine = provider.create_engine(plan, deps=deps, container=container)
    """

    LOOM_AI_ENGINE_API: ClassVar[int] = 1

    mcp_client_factory = staticmethod(create_mcp_client)
    """Session factory for ``mcp`` grants, read off the provider by the
    composition root so it never imports this engine (FR-016, FR-051)."""

    a2a_client_factory = staticmethod(create_a2a_client)
    """Client factory for ``a2a`` grants, read the same way."""

    def __init__(self, *, model_resolver: ModelResolver | None = None) -> None:
        self._resolve_model: ModelResolver = model_resolver or resolve_model

    def create_engine(
        self, plan: object, *, deps: DepsFactory, container: LoomContainer
    ) -> AgentEngine:
        """Build the engine serving one compiled plan.

        Args:
            plan: The compiled :class:`~loom.ai.compiler.AgentPlan`.
            deps: Per-invocation dependency factory.
            container: Application container.

        Returns:
            The engine serving this plan.

        Raises:
            TypeError: When ``plan`` is not an ``AgentPlan``.
            AgentCompilationError: When the vendor SDK the binding needs is not
                installed, or a required provider setting is missing.
        """
        if not isinstance(plan, AgentPlan):
            raise TypeError(f"expected an AgentPlan, got {type(plan).__name__}")
        model = self._resolve_model(plan.inference)
        toolsets = build_toolsets(plan, container)
        capabilities = build_capabilities(plan, container)
        output_type = build_output_type(plan)
        # The keyword is absent, not ``None``, when no mode is pinned: the
        # engine's default for ``output_type`` is ``str``, and passing ``None``
        # would override the resolution ``output_schema`` alone triggers.
        pinned: dict[str, Any] = {} if output_type is None else {"output_type": output_type}
        agent = Agent.from_spec(
            build_agent_spec(plan),
            model=model,
            deps_type=object,
            toolsets=toolsets or None,
            capabilities=capabilities or None,
            **pinned,
        )
        return PydanticAIEngine(plan=plan, agent=agent, deps=deps, container=container)

    def native_tool_support(self, target: InferenceTarget) -> frozenset[str]:
        """Return the provider tools the model bound to *target* admits.

        Satisfies :data:`~loom.ai.abc.NativeToolSupport`, which the bootstrap
        hands to the compiler so a ``native`` grant fails at compile time rather
        than on the first request.

        Args:
            target: Resolved model binding of the agent's role.

        Returns:
            The tool names this binding admits.

        Raises:
            AgentCompilationError: When the provider is unknown or its SDK is
                not installed.
        """
        return supported_native_tools(target)

    def supported_capability_kinds(self) -> frozenset[str]:
        """Capability kinds this adapter can serve.

        Derived from the one table that says how each kind is built, so a kind
        this adapter announces always has a builder behind it.

        Returns:
            The supported ``kind`` identifiers.
        """
        return SUPPORTED_KINDS
