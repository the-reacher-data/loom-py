"""The mechanism-agnostic authentication middleware.

The middleware knows nothing about tokens: it asks an :class:`Authenticator`
for an identity, refuses the request when it gets none, and guarantees the
identity is torn down afterwards — including when the handler raises, because
a worker task that inherits the previous caller is the leak this design exists
to prevent.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from loom.core.identity import Identity, current_identity
from loom.rest.auth import AuthenticationMiddleware, RequestCredentials

_SUBJECT_HEADER = "x-subject"
_MECHANISM = "test-header"
_PROTECTED = "/who"
_OPEN = "/health"

_Scope = dict[str, Any]
_Receive = Callable[[], Awaitable[dict[str, Any]]]
_Send = Callable[[dict[str, Any]], Awaitable[None]]


class _HeaderAuthenticator:
    """Authenticates from a plain header — deliberately not a JWT."""

    name = _MECHANISM
    provides_roles = True

    def __init__(self, *, roles: tuple[str, ...] = ()) -> None:
        self._roles = roles
        self.seen: list[RequestCredentials] = []

    async def authenticate(self, credentials: RequestCredentials) -> Identity | None:
        self.seen.append(credentials)
        subject = credentials.header(_SUBJECT_HEADER)
        if subject is None:
            return None
        return Identity(
            subject=subject,
            roles=self._roles,
            attributes={"email": f"{subject}@example.com"},
            mechanism=self.name,
        )


def _app(authenticator: _HeaderAuthenticator, *, exclude_paths: tuple[str, ...] = ()) -> FastAPI:
    app = FastAPI()

    @app.get(_PROTECTED)
    async def who() -> dict[str, Any]:
        identity = current_identity()
        return {
            "subject": identity.subject,
            "roles": list(identity.roles),
            "mechanism": identity.mechanism,
            "email": identity.attribute("email"),
        }

    @app.get(_OPEN)
    async def health() -> dict[str, str]:
        return {"subject": current_identity().subject}

    @app.get("/boom")
    async def boom() -> dict[str, str]:
        raise RuntimeError("handler exploded")

    app.add_middleware(
        AuthenticationMiddleware,
        authenticator=authenticator,
        exclude_paths=exclude_paths,
    )
    return app


def _client(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://testserver",
    )


async def _get(app: FastAPI, path: str, subject: str | None = None) -> httpx.Response:
    headers = {_SUBJECT_HEADER: subject} if subject is not None else {}
    async with _client(app) as client:
        return await client.get(path, headers=headers)


# ---------------------------------------------------------------------------
# Authentication outcome
# ---------------------------------------------------------------------------


async def test_an_accepted_caller_reaches_the_route_with_their_identity() -> None:
    """The identity the authenticator returned is what the request context carries."""
    response = await _get(_app(_HeaderAuthenticator(roles=("reader",))), _PROTECTED, "alice")
    assert response.json() == {
        "subject": "alice",
        "roles": ["reader"],
        "mechanism": _MECHANISM,
        "email": "alice@example.com",
    }


async def test_a_rejected_caller_never_reaches_the_route() -> None:
    """``None`` from the authenticator is a refusal, whatever the mechanism."""
    response = await _get(_app(_HeaderAuthenticator()), _PROTECTED)
    assert response.status_code == 401


async def test_the_401_carries_the_standard_error_body() -> None:
    """Refusals reuse the framework body so clients get code and trace_id."""
    detail = (await _get(_app(_HeaderAuthenticator()), _PROTECTED)).json()["detail"]
    assert {"code", "message", "trace_id"} <= set(detail)


async def test_the_401_carries_the_authentication_challenge() -> None:
    """RFC 9110 §11.6.1: a 401 must tell the client how to authenticate."""
    response = await _get(_app(_HeaderAuthenticator()), _PROTECTED)
    assert response.headers["www-authenticate"] == "Bearer"


async def test_excluded_paths_bypass_authentication() -> None:
    """Excluded paths are served without an identity, never with a forged one."""
    app = _app(_HeaderAuthenticator(), exclude_paths=(_OPEN,))
    response = await _get(app, _OPEN)
    assert (response.status_code, response.json()) == (200, {"subject": ""})


async def test_an_excluded_path_does_not_reach_the_authenticator() -> None:
    """Bypass means bypass: the mechanism is not consulted at all."""
    authenticator = _HeaderAuthenticator()
    await _get(_app(authenticator, exclude_paths=(_OPEN,)), _OPEN)
    assert authenticator.seen == []


# ---------------------------------------------------------------------------
# Credentials handed to the mechanism
# ---------------------------------------------------------------------------


async def test_the_authenticator_receives_the_request_path() -> None:
    """A mechanism may scope itself per path, so it must see which one it is."""
    authenticator = _HeaderAuthenticator()
    await _get(_app(authenticator), _PROTECTED, "alice")
    assert authenticator.seen[0].path == _PROTECTED


async def test_header_lookup_is_case_insensitive() -> None:
    """HTTP header names are case-insensitive; credentials must not pretend otherwise."""
    credentials = RequestCredentials(headers={"authorization": "Bearer x"}, path="/")
    assert credentials.header("AuThOrIzAtIoN") == "Bearer x"


async def test_absent_headers_read_as_none() -> None:
    """A missing header is ``None``, never an empty string to compare against."""
    credentials = RequestCredentials(headers={}, path="/")
    assert credentials.header("authorization") is None


# ---------------------------------------------------------------------------
# Teardown — the leak this middleware must not have
# ---------------------------------------------------------------------------


async def test_the_identity_is_reset_even_when_the_handler_raises() -> None:
    """Without the ``finally`` a reused task would inherit the previous caller."""
    app = _app(_HeaderAuthenticator())
    async with _client(app) as client:
        await client.get("/boom", headers={_SUBJECT_HEADER: "alice"})
    assert current_identity().subject == ""


async def test_the_handler_exception_is_not_swallowed() -> None:
    """Resetting the identity must not turn a crash into a silent success."""
    app = _app(_HeaderAuthenticator())
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=True)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        with pytest.raises(RuntimeError, match="handler exploded"):
            await client.get("/boom", headers={_SUBJECT_HEADER: "alice"})


async def test_concurrent_callers_never_cross() -> None:
    """Two requests in flight must each keep their own identity."""
    app = _app(_HeaderAuthenticator())
    async with _client(app) as client:
        alice, bob = await asyncio.gather(
            client.get(_PROTECTED, headers={_SUBJECT_HEADER: "alice"}),
            client.get(_PROTECTED, headers={_SUBJECT_HEADER: "bob"}),
        )
    assert (alice.json()["subject"], bob.json()["subject"]) == ("alice", "bob")


async def test_non_http_scopes_are_passed_through_untouched() -> None:
    """Lifespan and websocket scopes carry no credentials to authenticate."""
    seen: list[str] = []

    async def _inner(scope: _Scope, receive: _Receive, send: _Send) -> None:
        seen.append(scope["type"])

    middleware = AuthenticationMiddleware(_inner, authenticator=_HeaderAuthenticator())
    await middleware({"type": "lifespan"}, _noop_receive, _noop_send)

    assert seen == ["lifespan"]


async def _noop_receive() -> dict[str, Any]:
    return {"type": "lifespan.startup"}  # pragma: no cover - never awaited


async def _noop_send(message: dict[str, Any]) -> None:
    """Discard outbound ASGI messages."""
