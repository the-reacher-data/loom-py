"""Stateless JWT bearer authentication middleware.

Pure ASGI middleware — no FastAPI or Starlette dependency.  Compatible
with any ASGI-compliant framework and server.

Install the optional dependency with::

    pip install "loom-kernel[jwt]"
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from types import ModuleType
from typing import Any

import msgspec

from loom.core.tracing import get_trace_id
from loom.rest.auth.config import JwtAuthConfig

# ASGI type aliases
_Scope = dict[str, Any]
_Receive = Callable[[], Awaitable[dict[str, Any]]]
_Send = Callable[[dict[str, Any]], Awaitable[None]]
_ASGIApp = Callable[[_Scope, _Receive, _Send], Awaitable[None]]

_logger = logging.getLogger(__name__)

_UNAUTHORIZED_CODE = "unauthorized"
_UNAUTHORIZED_MESSAGE = "Authentication required: missing or invalid credentials."
_PYJWT_HINT = (
    "JwtAuthMiddleware requires the optional dependency 'pyjwt'. "
    "Install it with: pip install 'loom-kernel[jwt]'"
)


class JwtAuthMiddleware:
    """Pure ASGI middleware enforcing stateless JWT bearer authentication.

    On each HTTP request whose path is not in ``config.exclude_paths``:

    1. Extracts the token from the ``Authorization: Bearer`` header.
    2. Verifies signature and ``exp`` (required) plus ``nbf``, honouring
       ``leeway_seconds``; ``aud``/``iss`` are validated only when configured.
    3. On success, attaches the verified claims to
       ``scope["state"]["jwt_claims"]`` and forwards the request.
    4. On failure, responds ``401`` with the framework's standard error body
       (``code``, ``message``, ``trace_id``).  The message is deliberately
       generic for every failure mode (no oracle); the cryptographic reason
       is only logged at DEBUG and tokens/secrets are never logged.

    Verification is fully stateless: no server-side session storage and no
    remote JWKS fetch.  Non-HTTP scopes (WebSocket, lifespan) are passed
    through unchanged.

    Args:
        app: The ASGI application to wrap.
        config: Validated :class:`~loom.rest.auth.JwtAuthConfig`.

    Raises:
        ImportError: If the optional ``pyjwt`` dependency is not installed.

    Example — FastAPI::

        from loom.rest.auth import JwtAuthConfig, JwtAuthMiddleware

        config = JwtAuthConfig(secret="...", algorithms=("HS256",))
        app.add_middleware(JwtAuthMiddleware, config=config)
    """

    def __init__(self, app: _ASGIApp, *, config: JwtAuthConfig) -> None:
        self._app = app
        self._config = config
        self._jwt = _load_pyjwt()
        self._key = config.verification_key
        self._algorithms = list(config.algorithms)
        self._exclude_paths = frozenset(config.exclude_paths)
        self._decode_options: dict[str, Any] = {
            "require": ["exp"],
            "verify_aud": config.audience is not None,
        }

    async def __call__(self, scope: _Scope, receive: _Receive, send: _Send) -> None:
        if scope["type"] != "http" or scope["path"] in self._exclude_paths:
            await self._app(scope, receive, send)
            return

        claims = self._authenticate(scope)
        if claims is None:
            await _send_unauthorized(send)
            return

        state: dict[str, Any] = scope.setdefault("state", {})
        state["jwt_claims"] = claims
        await self._app(scope, receive, send)

    def _authenticate(self, scope: _Scope) -> dict[str, Any] | None:
        """Return the verified claims for the request, or ``None`` on failure."""
        token = _bearer_token(scope.get("headers", []))
        if token is None:
            return None
        return self._decode_claims(token)

    def _decode_claims(self, token: str) -> dict[str, Any] | None:
        """Verify *token* and return its claims; ``None`` when verification fails."""
        try:
            claims: dict[str, Any] = self._jwt.decode(
                token,
                self._key,
                algorithms=self._algorithms,
                audience=self._config.audience,
                issuer=self._config.issuer,
                leeway=self._config.leeway_seconds,
                options=self._decode_options,
            )
        except self._jwt.PyJWTError as exc:
            # DEBUG only, and never the token itself: no oracle in responses/logs.
            _logger.debug("JWT verification failed: %s: %s", type(exc).__name__, exc)
            return None
        return claims


def _load_pyjwt() -> ModuleType:
    """Import and return :mod:`jwt`, failing fast with an actionable hint.

    The import is local on purpose: ``pyjwt`` is an optional extra, and
    resolving it at middleware construction turns a missing dependency into
    a startup error instead of a broken API at first request.

    Returns:
        The imported ``jwt`` module.

    Raises:
        ImportError: If ``pyjwt`` is not installed.
    """
    try:
        import jwt
    except ImportError as exc:
        raise ImportError(_PYJWT_HINT) from exc
    return jwt


def _bearer_token(headers: list[tuple[bytes, bytes]]) -> str | None:
    """Return the ``Bearer`` token from ASGI *headers*, or ``None``.

    Args:
        headers: Raw ASGI headers list of ``(name_bytes, value_bytes)`` pairs.

    Returns:
        The token string, or ``None`` when the ``Authorization`` header is
        absent, uses another scheme, or carries no token.
    """
    header = next((value for key, value in headers if key.lower() == b"authorization"), None)
    if header is None:
        return None
    scheme, _, token = header.decode(errors="replace").strip().partition(" ")
    token = token.strip()
    if scheme.lower() != "bearer" or not token:
        return None
    return token


async def _send_unauthorized(send: _Send) -> None:
    """Send a 401 response using the framework's standard error body shape.

    The body mirrors :class:`~loom.rest.errors.HttpErrorMapper` details
    (``code``, ``message``, ``trace_id``) without importing the FastAPI
    layer, keeping this module pure ASGI.
    """
    detail = {
        "code": _UNAUTHORIZED_CODE,
        "message": _UNAUTHORIZED_MESSAGE,
        "trace_id": get_trace_id(),
    }
    body = msgspec.json.encode({"detail": detail})
    headers = [
        (b"content-type", b"application/json"),
        (b"content-length", str(len(body)).encode("ascii")),
        (b"www-authenticate", b"Bearer"),
    ]
    await send({"type": "http.response.start", "status": 401, "headers": headers})
    await send({"type": "http.response.body", "body": body})
