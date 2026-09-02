"""Pluggable authentication for the remote endpoints an agent calls out to.

One registry serves both outbound transports — the MCP servers of
``ai.mcp_servers`` and the remote agents of ``ai.a2a_agents``.  Loom ships
**no login flow of its own**.  A deployment names a strategy in that endpoint's
``auth.kind``; loom resolves the name in the entry-point group
:data:`REMOTE_AUTH_ENTRY_POINT_GROUP`, calls the registered object with the rest
of the block as keyword arguments, and hands the result to the client.  The
artifact keeps saying only ``server: <name>`` or ``agent: <name>``, so it moves
between environments unchanged whether that endpoint needs no credential, a
fixed header, or a token exchange.

**One group, named for what it authenticates, not for who called first.**  The
contract for a strategy is :class:`httpx.Auth` itself — not an abstraction of
ours — and nothing about ``Authorization: Bearer <token>`` is MCP-specific, so
a deployment that authenticates to one internal service registers its strategy
once and grants it to either transport.  The group was introduced in 1.7.0 as
``loom.ai.mcp_auth``, when MCP was its only consumer; keeping that name for
A2A strategies too would make it a lie for half its contents and would leave a
third party unable to tell which transports its registration is offered to, so
the group was renamed the release after it appeared, with no deprecation cycle
because it had no consumers yet.

Any existing ``httpx.Auth`` class in the ecosystem is registered as it stands,
with no adapter::

    [project.entry-points."loom.ai.remote_auth"]
    agent-session = "my_package.auth:AgentSessionAuth"

Loom registers three strategies, all thin delegations to what the libraries
already provide:

``oauth``
    Returns the sentinel the MCP client understands, so the client's own
    standard OAuth flow runs.  Loom implements none of it.  It is the one
    strategy that is not an ``httpx.Auth`` and therefore the one an A2A agent
    cannot use: :func:`shared_a2a_auth` refuses it by name rather than
    connecting without it.
``bearer``
    ``Authorization: Bearer <token>`` from a token the deployment resolves.
``static``
    Fixed headers, resolved by the deployment's secret resolver.

``headers_ref`` and ``auth`` are mutually exclusive on one endpoint, and
``auth: {kind: static, ...}`` is the long spelling of the shorthand: both read
the same ``Name=value`` payload through :func:`headers_from_ref`.  Reach for
``bearer`` whenever the credential is a token, since the composed header cannot
be written in configuration at all.

Nothing here imports ``httpx`` at module load: ``httpx`` reaches a deployment
with the MCP or A2A client, both optional, while the strategy-name check runs
during configuration decode in every deployment.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from functools import cache
from threading import Lock
from typing import TYPE_CHECKING, Any, cast

from loom.ai.errors import (
    AgentCompilationError,
    mcp_auth_strategy_invalid,
    mcp_headers_ref_invalid,
)
from loom.core.plugins.entrypoints import list_entry_points, select_entry_point

if TYPE_CHECKING:  # annotations only: importing the plan at runtime would
    # close a cycle, since the configuration this module validates is what the
    # compiler reads to build that plan.
    from collections.abc import Generator

    import httpx

    from loom.ai.compiler import CompiledRemoteAuth

REMOTE_AUTH_ENTRY_POINT_GROUP = "loom.ai.remote_auth"
"""Entry-point group every outbound authentication strategy registers under."""

_HEADER_SEPARATOR = "="

_MCP_SCOPE = "mcp"
_A2A_SCOPE = "a2a"


def registered_strategy_names() -> list[str]:
    """List the strategy names installed distributions register.

    Returns:
        Every name registered in :data:`REMOTE_AUTH_ENTRY_POINT_GROUP`, sorted,
        as the "not registered, available: ..." message reports them.
    """
    return sorted({ep.name for ep in list_entry_points(REMOTE_AUTH_ENTRY_POINT_GROUP)})


def is_strategy_registered(kind: str) -> bool:
    """Report whether ``kind`` resolves to a registered strategy.

    Called during configuration decode so an unknown strategy fails at compile
    time, naming what is installed, rather than at the first message in
    production.

    Args:
        kind: Strategy name from an endpoint's ``auth.kind``.

    Returns:
        ``True`` when a distribution registers ``kind``.
    """
    if not kind:
        return False
    return select_entry_point(REMOTE_AUTH_ENTRY_POINT_GROUP, kind, on_duplicate="error") is not None


def headers_from_ref(component: str, headers_ref: str | None) -> dict[str, str]:
    """Read the header a resolved ``headers_ref`` carries.

    The deployment's secret resolver has already run — ``${secrets:/path}`` is
    an OmegaConf resolver, so the value reaching loom is the payload, not the
    path.  That payload must be **one ``Name=value`` header pair**, and the
    shape is checked here, at start-up, rather than while the artifact compiles:
    configuration accepts any reference-shaped string, so a malformed payload is
    a start-up failure by name, not a compile-time one.  Several headers, a value
    carrying spaces, or a bearer token belong in a strategy — see
    :func:`bearer_token` — rather than in this shorthand.

    Args:
        component: Configuration path the failure is attributed to.
        headers_ref: Resolved payload, or ``None`` when the server declares none.

    Returns:
        The headers to send, empty when ``headers_ref`` is ``None``.

    Raises:
        AgentCompilationError: With ``MCP_HEADERS_REF_INVALID`` when the payload
            is not one ``Name=value`` pair.  The rejected value never appears in
            the message.

    Example::

        headers_from_ref("mcp server 'kb'", "X-API-Key=abc123")
    """
    if headers_ref is None:
        return {}
    name, separator, value = headers_ref.partition(_HEADER_SEPARATOR)
    if not separator or not name or not value:
        raise AgentCompilationError([mcp_headers_ref_invalid(component)])
    return {name: value}


def shared_mcp_auth(server: str, auth: CompiledRemoteAuth | None) -> httpx.Auth | str | None:
    """Return the authentication object of an MCP server, built once per server.

    **One instance per server, shared by every agent granted it.**  The
    credential belongs to the deployment, not to the agent: a renewing strategy
    holds a token, and one instance means one renewal rather than one per agent,
    and no burst of simultaneous logins when several agents start together.

    Args:
        server: Configured server name, the sharing key.
        auth: Compiled strategy name and settings, or ``None`` when the server
            declares none.

    Returns:
        What the MCP client's ``auth`` parameter accepts — an
        :class:`httpx.Auth`, the sentinel the client's own OAuth flow answers
        to, or ``None`` when the server declares no strategy.

    Raises:
        AgentCompilationError: With ``MCP_AUTH_STRATEGY_INVALID`` when the
            strategy cannot be loaded, rejects its settings, or returns
            something the MCP client cannot use.

    Example::

        auth = shared_mcp_auth("orders", capability.auth)
    """
    if auth is None:
        return None
    return _STRATEGIES.get(f"{_MCP_SCOPE}:{server}", auth)


def shared_a2a_auth(agent: str, auth: CompiledRemoteAuth | None) -> httpx.Auth | None:
    """Return the authentication object of a remote agent, built once per agent.

    Shares per configured agent exactly as :func:`shared_mcp_auth` shares per
    server, and from the same registry: the sharing keys are scoped by transport
    so that an MCP server and an A2A agent registered under the same name remain
    two credentials.

    Args:
        agent: Configured agent name, the sharing key.
        auth: Compiled strategy name and settings, or ``None`` when the agent
            declares none.

    Returns:
        The :class:`httpx.Auth` the A2A HTTP client presents, or ``None`` when
        the agent declares no strategy.

    Raises:
        AgentCompilationError: With ``MCP_AUTH_STRATEGY_INVALID`` when the
            strategy cannot be loaded, rejects its settings, or resolves to the
            MCP client's OAuth sentinel, which no HTTP client can present.

    Example::

        auth = shared_a2a_auth("market", capability.auth)
    """
    if auth is None:
        return None
    built = _STRATEGIES.get(f"{_A2A_SCOPE}:{agent}", auth)
    if isinstance(built, str):
        # ``oauth`` delegates to the MCP client's own flow, which the A2A
        # transport does not have. Refusing by name is the point: connecting
        # without the credential the deployment declared is the failure this
        # module exists to prevent.
        raise AgentCompilationError(
            [
                mcp_auth_strategy_invalid(
                    auth.kind,
                    "it delegates to the MCP client's own flow, which the A2A transport "
                    "cannot run; an A2A agent needs an httpx.Auth strategy",
                )
            ]
        )
    return built


def standard_oauth() -> str:
    """Delegate to the MCP client's own standard OAuth flow.

    Registered as the ``oauth`` strategy.  Loom implements no part of the flow:
    it returns the sentinel the client library answers to, so the OAuth support
    the MCP specification standardises is the client's, unchanged.

    Returns:
        The sentinel the MCP client recognises as "run your OAuth flow".
    """
    return "oauth"


def bearer_token(*, token_ref: str) -> httpx.Auth:
    """Send ``Authorization: Bearer <token>``, the most common MCP credential.

    Registered as the ``bearer`` strategy.  The strategy composes the header
    itself, which is the whole point of it: the composed value carries a space,
    and configuration refuses a space so that no literal credential can hide in
    it.  A token on its own — a JWT is base64url with dots — passes that test,
    so the deployment stores the token and loom writes the header.

    Args:
        token_ref: Resolved bearer token, never the composed header.

    Returns:
        An ``httpx.Auth`` that presents the token on every request.
    """
    return _static_headers_auth_class()({"Authorization": f"Bearer {token_ref}"})


def static_headers(*, headers_ref: str) -> httpx.Auth:
    """Attach fixed headers, resolved by the deployment's secret resolver.

    Registered as the ``static`` strategy.  It is the ``auth`` block's spelling
    of the ``headers_ref`` shorthand, for deployments that prefer every server
    to name a strategy.

    Args:
        headers_ref: Resolved ``Name=value`` payload, as ``headers_ref`` carries.

    Returns:
        An ``httpx.Auth`` that adds those headers to every request.

    Raises:
        AgentCompilationError: With ``MCP_HEADERS_REF_INVALID`` when the payload
            is not one ``Name=value`` pair.
    """
    headers = headers_from_ref("auth strategy 'static'", headers_ref)
    return _static_headers_auth_class()(headers)


class _SharedStrategies:
    """The one authentication instance of each remote endpoint, built on demand.

    Construction happens while the deployment starts up, from however many
    agents were granted the same endpoint; the lock makes that first build
    happen once, and every later caller receives the same object.  Keys are
    transport-scoped, so an MCP server and an A2A agent sharing a configured
    name do not share a credential.
    """

    def __init__(self) -> None:
        self._by_endpoint: dict[str, httpx.Auth | str] = {}
        self._lock = Lock()

    def get(self, endpoint: str, auth: CompiledRemoteAuth) -> httpx.Auth | str:
        with self._lock:
            existing = self._by_endpoint.get(endpoint)
            if existing is not None:
                return existing
            built = _build(auth)
            self._by_endpoint[endpoint] = built
            return built


def _build(auth: CompiledRemoteAuth) -> httpx.Auth | str:
    """Load the named strategy and construct it from its settings."""
    strategy = _load_strategy(auth.kind)
    settings = dict(auth.settings)
    try:
        built = strategy(**settings)
    except TypeError as exc:
        raise AgentCompilationError(
            [mcp_auth_strategy_invalid(auth.kind, f"it rejected its settings: {exc}")]
        ) from exc
    return _checked(auth.kind, built)


def _load_strategy(kind: str) -> Any:
    ep = select_entry_point(REMOTE_AUTH_ENTRY_POINT_GROUP, kind, on_duplicate="error")
    if ep is None:
        # Unreachable through configuration, which refuses an unregistered
        # strategy at decode; reached only if a distribution is uninstalled
        # between decode and start-up.
        raise AgentCompilationError([mcp_auth_strategy_invalid(kind, "it is not registered")])
    return ep.load()


def _checked(kind: str, built: object) -> httpx.Auth | str:
    """Accept only what a client can use, structurally, never by class.

    The handshake mirrors the engine registry's: an ``auth_flow`` attribute is
    what ``httpx`` calls, so a strategy satisfying that contract is accepted
    whichever ``httpx`` it was written against.
    """
    if isinstance(built, str) or callable(getattr(built, "auth_flow", None)):
        return cast("httpx.Auth | str", built)
    raise AgentCompilationError(
        [mcp_auth_strategy_invalid(kind, "it returned no httpx.Auth and no client sentinel")]
    )


@cache
def _static_headers_auth_class() -> Callable[[Mapping[str, str]], httpx.Auth]:
    """Build the ``static`` strategy's class on first use.

    ``httpx`` is not a loom dependency — it arrives with the MCP client — so the
    class cannot be defined at module level without breaking every deployment
    that declares no ``mcp`` grant.
    """
    import httpx

    class _StaticHeadersAuth(httpx.Auth):
        """Adds the same headers to every request, and never renews them."""

        def __init__(self, headers: Mapping[str, str]) -> None:
            self._headers = dict(headers)

        def auth_flow(
            self, request: httpx.Request
        ) -> Generator[httpx.Request, httpx.Response, None]:
            request.headers.update(self._headers)
            yield request

    return _StaticHeadersAuth


_STRATEGIES = _SharedStrategies()
"""Process-wide sharing of one authentication instance per remote endpoint."""


__all__: Sequence[str] = [
    "REMOTE_AUTH_ENTRY_POINT_GROUP",
    "bearer_token",
    "headers_from_ref",
    "is_strategy_registered",
    "registered_strategy_names",
    "shared_a2a_auth",
    "shared_mcp_auth",
    "standard_oauth",
    "static_headers",
]
