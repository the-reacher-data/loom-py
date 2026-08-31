"""Entry-point target of the pydantic-ai engine (group ``loom.ai.engines``).

The provider is what the ``ai.engine: pydantic-ai`` setting resolves to. It
builds one engine per plan — called exactly once per plan by
:class:`~loom.ai.runtime.AgentRuntime`, in ``__aenter__``, never per request —
and answers which capability kinds this adapter can serve.
"""

from __future__ import annotations

from typing import ClassVar

from pydantic_ai import Agent

from loom.ai.abc import AgentEngine, DepsFactory
from loom.ai.compiler import AgentPlan
from loom.ai.engines.pydantic_ai._engine import PydanticAIEngine
from loom.ai.engines.pydantic_ai._models import ModelResolver, resolve_model
from loom.ai.engines.pydantic_ai._spec import build_agent_spec
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
        agent = Agent.from_spec(build_agent_spec(plan), model=model, deps_type=object)
        return PydanticAIEngine(plan=plan, agent=agent, deps=deps, container=container)

    def supported_capability_kinds(self) -> frozenset[str]:
        """Capability kinds this adapter can serve.

        Empty in this delivery step: the engine runs pure-language agents, and
        the capability toolsets (``usecase``, ``sql``, ``mcp``, ``skills``,
        ``python``, ``a2a``) are the next one. Returning the kinds before they
        exist would compile a grant the engine cannot honour.

        Returns:
            The supported ``kind`` identifiers.
        """
        return frozenset()
