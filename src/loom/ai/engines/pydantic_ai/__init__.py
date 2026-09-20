"""pydantic-ai engine adapter, behind the ``ai-pydantic`` extra.

Importing this package requires ``pydantic-ai-slim``; nothing in ``loom.ai``
imports it. The runtime reaches :class:`PydanticAIEngineProvider` through the
``loom.ai.engines`` entry point named ``pydantic-ai``.

The adapter is *native-inside*: an :class:`~loom.ai.compiler.AgentPlan` is
translated into a ``pydantic_ai.AgentSpec`` and handed to
``Agent.from_spec()``. Loom wraps none of the engine's primitives; it owns
only what the engine does not do — binding the model, validating the output
and classifying failures.

:func:`create_a2a_client` is exported beside the provider because it is not
part of the engine contract: it is the
:data:`~loom.ai.runtime.A2AClientFactory` a composition root hands to
:class:`~loom.ai.runtime.AgentRuntime`, so that an outbound grant is validated
against the live remote at start-up rather than on its first use. The MCP
counterpart is ``PydanticAIEngineProvider.mcp_client_factory`` rather than a
free function, because the session it opens is the very toolset the run path
uses: one connection per server for the whole worker. :func:`create_mcp_client`
opens a session of its own and is for diagnostics, never for wiring the
runtime.

:func:`native_agent` is the escape hatch out of loom: the agent this adapter
built, handed to calling code so it can drive pydantic-ai directly for what
loom's neutral surface does not serve. Being *this* package's export is the
point — a caller reaching it has imported an engine on purpose.
"""

from __future__ import annotations

from loom.ai.engines.pydantic_ai._a2a import create_a2a_client
from loom.ai.engines.pydantic_ai._escape import NativeAgent, native_agent
from loom.ai.engines.pydantic_ai._mcp import create_mcp_client
from loom.ai.engines.pydantic_ai.provider import PydanticAIEngineProvider

__all__ = [
    "NativeAgent",
    "PydanticAIEngineProvider",
    "create_a2a_client",
    "create_mcp_client",
    "native_agent",
]
