"""Tests for the native stateless ``JwtAuthMiddleware`` and its config (spec §3/§7.10)."""

from __future__ import annotations

import base64
import json
import sys
import time
from collections.abc import Awaitable, Callable
from typing import Any

import jwt as pyjwt
import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from loom.core.config import ConfigContext
from loom.core.config.errors import ConfigError
from loom.rest.auth import JwtAuthConfig, JwtAuthMiddleware

_SECRET = "unit-test-secret"

_Scope = dict[str, Any]
_Receive = Callable[[], Awaitable[dict[str, Any]]]
_Send = Callable[[dict[str, Any]], Awaitable[None]]


async def _noop_asgi_app(scope: _Scope, receive: _Receive, send: _Send) -> None:
    """Minimal ASGI app used only as a wrapping target."""


def _config(**overrides: Any) -> JwtAuthConfig:
    """Build a valid HS256 config with targeted overrides."""
    params: dict[str, Any] = {"secret": _SECRET, "algorithms": ("HS256",)}
    params.update(overrides)
    return JwtAuthConfig(**params)


def _token(*, secret: str = _SECRET, exp_offset: int = 3600, **claims: Any) -> str:
    """Encode an HS256 token with a relative expiry and extra claims."""
    payload: dict[str, Any] = {"sub": "user-1", "exp": int(time.time()) + exp_offset}
    payload.update(claims)
    return pyjwt.encode(payload, secret, algorithm="HS256")


def _unsigned_token(**claims: Any) -> str:
    """Craft an ``alg: none`` token manually (pyjwt-independent)."""

    def _b64(part: dict[str, Any]) -> str:
        return base64.urlsafe_b64encode(json.dumps(part).encode()).rstrip(b"=").decode()

    payload: dict[str, Any] = {"sub": "user-1", "exp": int(time.time()) + 3600}
    payload.update(claims)
    return f"{_b64({'alg': 'none', 'typ': 'JWT'})}.{_b64(payload)}."


def _client(config: JwtAuthConfig) -> TestClient:
    """FastAPI app exposing the middleware-attached claims at GET /claims."""
    app = FastAPI()

    @app.get("/claims")
    async def read_claims(request: Request) -> dict[str, Any]:
        return dict(request.scope["state"]["jwt_claims"])

    app.add_middleware(JwtAuthMiddleware, config=config)
    return TestClient(app, raise_server_exceptions=False)


def _get(client: TestClient, token: str | None) -> Any:
    headers = {"Authorization": f"Bearer {token}"} if token is not None else {}
    return client.get("/claims", headers=headers)


def _hide_modules(monkeypatch: pytest.MonkeyPatch, prefix: str) -> None:
    """Simulate the absence of an optional dependency by nulling its modules."""
    for name in list(sys.modules):
        if name == prefix or name.startswith(f"{prefix}."):
            monkeypatch.setitem(sys.modules, name, None)
    monkeypatch.setitem(sys.modules, prefix, None)


# ---------------------------------------------------------------------------
# Config validation (fail-fast)
# ---------------------------------------------------------------------------


def test_config_parses_from_the_app_rest_auth_jwt_section() -> None:
    """The YAML section binds with the documented defaults (leeway 0, docs excluded)."""
    ctx = ConfigContext.from_dict(
        {"app": {"rest": {"auth": {"jwt": {"secret": _SECRET, "algorithms": ["HS256"]}}}}}
    )
    config = ctx.section("app.rest.auth.jwt", JwtAuthConfig)
    assert (config.leeway_seconds, "/openapi.json" in config.exclude_paths) == (0, True)


def test_config_fails_when_secret_and_public_key_are_both_set() -> None:
    """Exactly one of ``secret``/``public_key`` is allowed (fail-fast)."""
    with pytest.raises(ConfigError):
        JwtAuthConfig(
            secret=_SECRET,
            public_key="-----BEGIN PUBLIC KEY-----",
            algorithms=("HS256",),
        )


def test_config_fails_when_neither_secret_nor_public_key_is_set() -> None:
    """A key source is mandatory: no ``secret`` and no ``public_key`` => ConfigError."""
    with pytest.raises(ConfigError):
        JwtAuthConfig(algorithms=("HS256",))


def test_config_fails_when_algorithms_is_empty() -> None:
    """An empty ``algorithms`` allowlist is rejected fail-fast."""
    with pytest.raises(ConfigError):
        JwtAuthConfig(secret=_SECRET, algorithms=())


def test_config_forbids_the_none_algorithm_even_alongside_others() -> None:
    """``none`` is always forbidden in the allowlist, even mixed with HS256."""
    with pytest.raises(ConfigError):
        JwtAuthConfig(secret=_SECRET, algorithms=("HS256", "none"))


def test_config_fails_when_hs_algorithm_is_paired_with_public_key() -> None:
    """HS* requires ``secret``: pairing it with ``public_key`` is incoherent."""
    with pytest.raises(ConfigError):
        JwtAuthConfig(public_key="-----BEGIN PUBLIC KEY-----", algorithms=("HS256",))


def test_config_fails_when_rs_algorithm_is_paired_with_secret() -> None:
    """RS*/ES* require ``public_key``: pairing them with ``secret`` is incoherent."""
    with pytest.raises(ConfigError):
        JwtAuthConfig(secret=_SECRET, algorithms=("RS256",))


# ---------------------------------------------------------------------------
# Middleware behaviour
# ---------------------------------------------------------------------------


def test_valid_token_passes_and_claims_land_in_scope_state() -> None:
    """A valid HS256 token reaches the route with claims in scope['state']."""
    response = _get(_client(_config()), _token())
    assert (response.status_code, response.json()["sub"]) == (200, "user-1")


def test_expired_token_returns_401() -> None:
    """An expired ``exp`` claim is rejected with 401."""
    response = _get(_client(_config()), _token(exp_offset=-3600))
    assert response.status_code == 401


def test_invalid_signature_returns_401() -> None:
    """A token signed with a different secret is rejected with 401."""
    response = _get(_client(_config()), _token(secret="other-secret"))
    assert response.status_code == 401


def test_wrong_audience_returns_401() -> None:
    """When ``audience`` is configured, a mismatching ``aud`` claim is rejected."""
    response = _get(_client(_config(audience="loom-api")), _token(aud="other-api"))
    assert response.status_code == 401


def test_matching_audience_passes() -> None:
    """When ``audience`` is configured, a matching ``aud`` claim is accepted."""
    response = _get(_client(_config(audience="loom-api")), _token(aud="loom-api"))
    assert response.status_code == 200


def test_missing_authorization_header_returns_401() -> None:
    """No ``Authorization`` header means no access (fail-closed)."""
    response = _get(_client(_config()), None)
    assert response.status_code == 401


@pytest.mark.parametrize(
    "header_value",
    ["Token abc", "Bearer", "bearer-xyz"],
    ids=["wrong-scheme", "bearer-without-token", "not-a-scheme"],
)
def test_malformed_authorization_header_returns_401(header_value: str) -> None:
    """A malformed ``Authorization`` header is rejected with 401."""
    response = _client(_config()).get("/claims", headers={"Authorization": header_value})
    assert response.status_code == 401


def test_alg_none_token_is_rejected() -> None:
    """An unsigned ``alg: none`` token is always rejected with 401."""
    response = _get(_client(_config()), _unsigned_token())
    assert response.status_code == 401


def test_default_exclude_paths_bypass_authentication() -> None:
    """``exclude_paths`` defaults keep /docs reachable without a token."""
    response = _client(_config()).get("/docs")
    assert response.status_code == 200


def test_401_body_uses_the_standard_error_shape() -> None:
    """The 401 body carries the framework error keys: code, message, trace_id."""
    response = _get(_client(_config()), None)
    assert {"code", "message", "trace_id"} <= set(response.json()["detail"])


def test_401_message_does_not_leak_the_cryptographic_reason() -> None:
    """Expired and bad-signature failures share one generic message (no oracle)."""
    client = _client(_config())
    expired = _get(client, _token(exp_offset=-3600)).json()["detail"]["message"]
    bad_signature = _get(client, _token(secret="other-secret")).json()["detail"]["message"]
    assert (expired == bad_signature, "signature" in expired.lower()) == (True, False)


def test_repr_redacts_the_shared_secret() -> None:
    """``repr(JwtAuthConfig)`` never exposes the shared secret."""
    rendered = repr(_config())
    assert (_SECRET in rendered, "'***'" in rendered) == (False, True)


def test_missing_pyjwt_extra_fails_at_startup_with_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Config present + pyjwt absent aborts startup with a 'loom-kernel[jwt]' hint."""
    config = _config()
    _hide_modules(monkeypatch, "jwt")
    with pytest.raises(ImportError, match=r"loom-kernel\[jwt\]"):
        JwtAuthMiddleware(_noop_asgi_app, config=config)
