"""Entry-point target of the pydantic-ai engine (group ``loom.ai.engines``).

The provider is what the ``ai.engine: pydantic-ai`` setting resolves to. It
builds one engine per plan — called exactly once per plan by
:class:`~loom.ai.runtime.AgentRuntime`, in ``__aenter__``, never per request —
and answers which capability kinds this adapter can serve.

The plan's output schema reaches the engine through the spec; a pinned
``output_mode`` on the model binding reaches it as ``output_type=`` on
``Agent.from_spec`` (see :func:`~loom.ai.engines.pydantic_ai._spec.build_output_type`),
and that keyword is absent when no mode is pinned.

The plan's ``instructions`` and ``description`` reach the agent as keyword
arguments rather than inside the spec, so a text containing ``{{`` stays a
literal instead of selecting the engine's template branch (see
:mod:`~loom.ai.engines.pydantic_ai._spec`).

The plan's ``output_check`` is registered on the built agent by
:mod:`~loom.ai.engines.pydantic_ai._checks`, which owns the translation from
loom's return contract to the engine's retry signal, and which also refuses,
before anything is built, the one binding that cannot serve a check: a pinned
``output_mode: native``, where a rejection would show the caller the answer
twice.

The plan's ``dynamic_instructions`` factory is called, and the provider it
returns registered, by :mod:`~loom.ai.engines.pydantic_ai._instructions`. Both
registrations happen on the already-built agent, so the engine factory keeps
one responsibility.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from pydantic_ai import Agent

from loom.ai.abc import AgentEngine, DepsFactory
from loom.ai.compiler import AgentPlan, CompiledMcpCapability
from loom.ai.engines.pydantic_ai._a2a import create_a2a_client
from loom.ai.engines.pydantic_ai._capabilities import (
    SUPPORTED_KINDS,
    build_capabilities,
    build_toolsets,
)
from loom.ai.engines.pydantic_ai._checks import (
    register_output_check,
    reject_unservable_output_check,
)
from loom.ai.engines.pydantic_ai._engine import PydanticAIEngine
from loom.ai.engines.pydantic_ai._instructions import register_dynamic_instructions
from loom.ai.engines.pydantic_ai._mcp import SharedMcpToolsets
from loom.ai.engines.pydantic_ai._models import ModelResolver, resolve_model
from loom.ai.engines.pydantic_ai._native import supported_native_tools
from loom.ai.engines.pydantic_ai._spec import build_agent_spec, build_output_type
from loom.ai.inference import InferenceTarget
from loom.core.di import LoomContainer

if TYPE_CHECKING:
    from contextlib import AbstractAsyncContextManager

    from loom.ai.runtime import McpSession


class PydanticAIEngineProvider:
    """Builds pydantic-ai engines from compiled plans.

    Args:
        model_resolver: Builds the engine model from the plan's resolved
            binding. Defaults to the provider bindings of
            :mod:`~loom.ai.engines.pydantic_ai._models`; a deployment supplies
            one to inject a preconfigured vendor client, and the shared engine
            contract suite supplies one to exercise this adapter with no
            network and no credentials (FR-048).

    One instance serves one runtime lifecycle. It holds the worker's shared
    MCP toolsets, which are never evicted, so re-entering the same provider
    from a second event loop would reuse a connection lock bound to the first.
    ``create_app`` builds one provider per application.

    Attributes:
        LOOM_AI_ENGINE_API: Handshake version, read with ``getattr`` on load.

    Example::

        provider = PydanticAIEngineProvider()
        engine = provider.create_engine(plan, deps=deps, container=container)
    """

    LOOM_AI_ENGINE_API: ClassVar[int] = 2

    a2a_client_factory = staticmethod(create_a2a_client)
    """Client factory for ``a2a`` grants, read off the provider by the
    composition root so it never imports this engine (FR-016, FR-051)."""

    def __init__(self, *, model_resolver: ModelResolver | None = None) -> None:
        self._resolve_model: ModelResolver = model_resolver or resolve_model
        self._mcp = SharedMcpToolsets()

    def mcp_client_factory(
        self, capability: CompiledMcpCapability
    ) -> AbstractAsyncContextManager[McpSession]:
        """Open the worker's shared session for one ``mcp`` grant.

        Read off the provider by the composition root the same way
        ``a2a_client_factory`` is (FR-016, FR-051). It is an instance method,
        not a free function, because the session it opens is the very toolset
        :meth:`create_engine` puts behind the capability boundary: one
        connection per server for the whole worker, not one for start-up plus
        one per agent (FR-026).

        Args:
            capability: Compiled grant naming the server to reach.

        Returns:
            The not-yet-opened client; entering it connects the server.
        """
        return self._mcp.open(capability)

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
                installed, a required provider setting is missing, a declared
                factory fails to produce what it promised, or the artifact
                declares an ``output_check`` the binding's pinned output mode
                cannot serve.
        """
        if not isinstance(plan, AgentPlan):
            raise TypeError(f"expected an AgentPlan, got {type(plan).__name__}")
        reject_unservable_output_check(plan)
        agent = self._build_agent(plan, container)
        register_output_check(agent, plan.output_check)
        register_dynamic_instructions(agent, plan, container, mcp=self._mcp)
        return PydanticAIEngine(plan=plan, agent=agent, deps=deps, container=container)

    def _build_agent(self, plan: AgentPlan, container: LoomContainer) -> Agent[Any, Any]:
        """Assemble the engine agent from the plan's spec, model and toolsets.

        The template-typed ``instructions`` and ``description`` travel as
        keyword arguments rather than inside the spec, for the reason
        :mod:`~loom.ai.engines.pydantic_ai._spec` gives.

        Args:
            plan: Compiled plan to build.
            container: Application container the factories receive.

        Returns:
            The built agent, before anything is registered on it.
        """
        output_type = build_output_type(plan)
        # The keyword is absent, not ``None``, when no mode is pinned: the
        # engine's default for ``output_type`` is ``str``, and passing ``None``
        # would override the resolution ``output_schema`` alone triggers.
        pinned: dict[str, Any] = {} if output_type is None else {"output_type": output_type}
        toolsets = build_toolsets(plan, container, mcp=self._mcp)
        capabilities = build_capabilities(plan, container)
        return Agent.from_spec(
            build_agent_spec(plan),
            model=self._resolve_model(plan.inference),
            deps_type=object,
            instructions=plan.instructions,
            description=plan.description,
            toolsets=toolsets or None,
            capabilities=capabilities or None,
            **pinned,
        )

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
