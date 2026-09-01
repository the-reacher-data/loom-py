"""Outbound A2A transport: this agent calling a remote one (T147).

Two entry points, one connection recipe:

* :func:`create_a2a_client` is the :data:`~loom.ai.runtime.A2AClientFactory`
  the runtime opens at start-up. Entering it fetches the remote card, checks
  the granted skills against it and hands back a usable session, so an
  unreachable or mismatched agent fails start-up as ``A2A_AGENT_UNREACHABLE``
  instead of surfacing on the first delegation.
* :func:`send_to_remote_agent` performs one delegation. It opens the same
  context manager per call, because ``build_toolsets`` is synchronous and the
  engine has no channel to the session the runtime opened: sharing it would
  need a lifecycle handoff that does not exist yet. The cost is one card fetch
  per delegation, bounded by ``tool_timeout_ms`` like the call it precedes, and
  it buys a stateless toolset — no cached client, no connection nobody closes.

**The remote agent's reply is untrusted input (FR-044a).** It is returned as a
tool *value*, never merged into instructions; it is never logged; and no byte
of it reaches an error message, so a remote agent cannot dictate what the
calling model or the caller is told about a failure.

Everything the ``a2a-sdk`` provides is imported inside the function that needs
it: the SDK ships behind the ``ai-a2a`` extra, and importing it at module load
would break every deployment that declares no ``a2a`` grant.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib.util import find_spec
from typing import TYPE_CHECKING, Final

from loom.ai.compiler import CompiledA2ACapability
from loom.ai.errors import AgentCompilationError, provider_not_installed

if TYPE_CHECKING:
    from a2a.client import Client
    from a2a.types.a2a_pb2 import AgentCard

_ACCEPTED_OUTPUT_MODES: Final[tuple[str, ...]] = ("text/plain", "application/json")
"""Output modes named explicitly: some servers refuse a request that omits them."""


def require_a2a_sdk() -> None:
    """Fail the build when the ``ai-a2a`` extra is not installed.

    Checked with :func:`importlib.util.find_spec` rather than an import so the
    build pays nothing and the SDK is still loaded lazily, inside the call that
    needs it.

    Raises:
        AgentCompilationError: When the ``a2a-sdk`` distribution is absent.
    """
    if find_spec("a2a") is None:
        raise AgentCompilationError([provider_not_installed("a2a", "ai-a2a")])


@asynccontextmanager
async def create_a2a_client(capability: CompiledA2ACapability) -> AsyncIterator[Client]:
    """Open one session against a remote agent, card fetched and checked.

    Satisfies :data:`~loom.ai.runtime.A2AClientFactory`: nothing happens until
    the context is entered, so the runtime's start-up deadline bounds the whole
    of it and a failure is reported as a coded start-up issue.

    Args:
        capability: Compiled grant carrying the validated ``https://`` URL and
            the optional subset of skills the artifact delegates to.

    Yields:
        The connected client, closed together with its HTTP client on exit.

    Raises:
        AgentCardResolutionError: When the card cannot be fetched, decoded or
            validated.
        ValueError: When the card advertises no transport this client speaks,
            or does not advertise a granted skill.

    Example::

        runtime = AgentRuntime(..., a2a_client_factory=create_a2a_client)
    """
    import httpx
    from a2a.client import A2ACardResolver, ClientConfig, ClientFactory

    async with httpx.AsyncClient() as http_client:
        card = await A2ACardResolver(http_client, capability.url).get_agent_card()
        _reject_ungranted_card(capability, card)
        config = ClientConfig(
            streaming=False,
            httpx_client=http_client,
            accepted_output_modes=list(_ACCEPTED_OUTPUT_MODES),
        )
        async with ClientFactory(config).create(card) as client:
            yield client


async def send_to_remote_agent(capability: CompiledA2ACapability, prompt: str) -> str:
    """Delegate one prompt to a remote agent and return its reply text.

    Args:
        capability: Compiled grant naming the remote agent.
        prompt: Text the calling model asks the remote agent to act on.

    Returns:
        The concatenated text of the remote agent's reply. It is untrusted
        data: the caller presents it as a tool value and never as instruction.

    Raises:
        Exception: Whatever the transport raises. The caller maps it to a coded
            failure; this function deliberately does not, so that the mapping
            lives with every other capability's mapping.
    """
    async with create_a2a_client(capability) as client:
        return await _reply_text(client, prompt)


async def _reply_text(client: Client, prompt: str) -> str:
    """Send one message and join the text of every response it produces."""
    from a2a.helpers import get_stream_response_text, new_text_message
    from a2a.types.a2a_pb2 import Role, SendMessageRequest

    request = SendMessageRequest(message=new_text_message(prompt, role=Role.ROLE_USER))
    chunks = [get_stream_response_text(response) async for response in client.send_message(request)]
    return "\n".join(chunk for chunk in chunks if chunk)


def _reject_ungranted_card(capability: CompiledA2ACapability, card: AgentCard) -> None:
    """Refuse a card that does not advertise every skill the artifact grants.

    Only names the artifact already carries are reported: no text of the remote
    card reaches the start-up issue.
    """
    granted = capability.skills
    if not granted:
        return
    advertised = {skill.id for skill in card.skills}
    missing = tuple(name for name in granted if name not in advertised)
    if missing:
        raise ValueError(f"the card does not advertise the granted skills: {', '.join(missing)}")
