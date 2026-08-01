"""Mechanism-agnostic authentication middleware.

Pure ASGI middleware — no FastAPI or Starlette dependency.  Compatible with
any ASGI-compliant framework and server.

The middleware owns the request-scoped concerns (path exclusions, the ``401``
response, and the identity context lifecycle) while the mechanism owns
verification.  Splitting them is what lets an application swap JWT for mutual
TLS without touching a single authorization rule.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import Any

import msgspec

from loom.core.errors.codes import ErrorCode
from loom.core.identity import reset_identity, set_identity
from loom.core.tracing import get_trace_id
from loom.rest.auth.abc import Authenticator, RequestCredentials
from loom.rest.auth.config import JwtAuthConfig
from loom.rest.auth.jwt import JwtAuthenticator
from loom.rest.constants import BEARER_CHALLENGE

# ASGI type aliases
_Scope = dict[str, Any]
_Receive = Callable[[], Awaitable[dict[str, Any]]]
_Send = Callable[[dict[str, Any]], Awaitable[None]]
_ASGIApp = Callable[[_Scope, _Receive, _Send], Awaitable[None]]

_HTTP_SCOPE = "http"
_UNAUTHORIZED_MESSAGE = "Authentication required: missing or invalid credentials."


class AuthenticationMiddleware:
    """Authenticates every HTTP request through a pluggable mechanism.

    On each HTTP request whose path is not excluded:

    1. Builds :class:`~loom.rest.auth.abc.RequestCredentials` from the ASGI
       scope.
    2. Asks the :class:`~loom.rest.auth.abc.Authenticator` for an identity.
    3. On refusal, answers ``401`` with the framework's standard error body
       and a ``WWW-Authenticate`` challenge.  The message is deliberately
       generic for every failure mode (no oracle).
    4. On success, installs the identity for the duration of the request and
       restores the previous one in a ``finally`` — without it, a reused
       worker task would inherit the previous caller.

    Non-HTTP scopes (WebSocket, lifespan) are passed through unchanged.

    Args:
        app: The ASGI application to wrap.
        authenticator: Mechanism that verifies callers.
        exclude_paths: Exact request paths served without authentication.

    Example::

        app.add_middleware(
            AuthenticationMiddleware,
            authenticator=MyApiKeyAuthenticator(store),
            exclude_paths=("/health",),
        )
    """

    def __init__(
        self,
        app: _ASGIApp,
        *,
        authenticator: Authenticator,
        exclude_paths: Sequence[str] = (),
    ) -> None:
        self._app = app
        self._authenticator = authenticator
        self._exclude_paths = frozenset(exclude_paths)

    async def __call__(self, scope: _Scope, receive: _Receive, send: _Send) -> None:
        """Authenticate the request, then delegate to the wrapped application."""
        if scope["type"] != _HTTP_SCOPE or scope["path"] in self._exclude_paths:
            await self._app(scope, receive, send)
            return

        identity = await self._authenticator.authenticate(_credentials(scope))
        if identity is None:
            await send_unauthorized(send)
            return

        token = set_identity(identity)
        try:
            await self._app(scope, receive, send)
        finally:
            reset_identity(token)


class JwtAuthMiddleware:
    """Stateless JWT bearer authentication, as a ready-made middleware.

    Thin composition over :class:`AuthenticationMiddleware` and
    :class:`~loom.rest.auth.jwt.JwtAuthenticator`: it exists so applications
    that only need JWT wire one class instead of two.

    Args:
        app: The ASGI application to wrap.
        config: Validated :class:`~loom.rest.auth.config.JwtAuthConfig`.

    Raises:
        ImportError: If the optional ``pyjwt`` dependency is not installed.

    Example — FastAPI::

        from loom.rest.auth import JwtAuthConfig, JwtAuthMiddleware

        config = JwtAuthConfig(secret="...", algorithms=("HS256",))
        app.add_middleware(JwtAuthMiddleware, config=config)
    """

    def __init__(self, app: _ASGIApp, *, config: JwtAuthConfig) -> None:
        self._delegate = AuthenticationMiddleware(
            app,
            authenticator=JwtAuthenticator(config),
            exclude_paths=config.exclude_paths,
        )

    async def __call__(self, scope: _Scope, receive: _Receive, send: _Send) -> None:
        """Delegate to the generic authentication middleware."""
        await self._delegate(scope, receive, send)


def _credentials(scope: _Scope) -> RequestCredentials:
    """Adapt an ASGI scope to the transport-free credentials contract."""
    headers: list[tuple[bytes, bytes]] = scope.get("headers", [])
    client = scope.get("client")
    return RequestCredentials(
        headers={key.decode("latin-1"): value.decode("latin-1") for key, value in headers},
        path=scope.get("path", ""),
        client_host=client[0] if client else None,
    )


async def send_unauthorized(send: _Send) -> None:
    """Send a ``401`` using the framework's standard error body shape.

    The body mirrors :class:`~loom.rest.errors.HttpErrorMapper` details
    (``code``, ``message``, ``trace_id``) without importing the FastAPI layer,
    keeping this module pure ASGI.

    Args:
        send: ASGI send callable of the request being refused.
    """
    detail = {
        "code": ErrorCode.UNAUTHENTICATED.value,
        "message": _UNAUTHORIZED_MESSAGE,
        "trace_id": get_trace_id(),
    }
    body = msgspec.json.encode({"detail": detail})
    headers = [
        (b"content-type", b"application/json"),
        (b"content-length", str(len(body)).encode("ascii")),
        *(
            (name.lower().encode("ascii"), value.encode("ascii"))
            for name, value in BEARER_CHALLENGE.items()
        ),
    ]
    await send({"type": "http.response.start", "status": 401, "headers": headers})
    await send({"type": "http.response.body", "body": body})
