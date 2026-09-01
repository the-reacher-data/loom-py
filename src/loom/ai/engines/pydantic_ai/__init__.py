"""pydantic-ai engine adapter, behind the ``ai-pydantic`` extra.

Importing this package requires ``pydantic-ai-slim``; nothing in ``loom.ai``
imports it. The runtime reaches :class:`PydanticAIEngineProvider` through the
``loom.ai.engines`` entry point named ``pydantic-ai``.

The adapter is *native-inside*: an :class:`~loom.ai.compiler.AgentPlan` is
translated into a ``pydantic_ai.AgentSpec`` and handed to
``Agent.from_spec()``. Loom wraps none of the engine's primitives; it owns
only what the engine does not do — binding the model, validating the output
and classifying failures.

:func:`create_a2a_client` and :func:`create_mcp_client` are exported beside the
provider because they are not part of the engine contract: they are the
:data:`~loom.ai.runtime.A2AClientFactory` and
:data:`~loom.ai.runtime.McpClientFactory` a composition root hands to
:class:`~loom.ai.runtime.AgentRuntime`, so that an outbound grant is validated
against the live remote — the card of a remote agent, the tool catalogue of an
MCP server — at start-up rather than on its first use.
"""

from __future__ import annotations

from loom.ai.engines.pydantic_ai._a2a import create_a2a_client
from loom.ai.engines.pydantic_ai._mcp import create_mcp_client
from loom.ai.engines.pydantic_ai.provider import PydanticAIEngineProvider

__all__ = ["PydanticAIEngineProvider", "create_a2a_client", "create_mcp_client"]
