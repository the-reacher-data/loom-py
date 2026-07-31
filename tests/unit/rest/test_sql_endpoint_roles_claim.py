"""Identity-bound SQL role resolution at the REST edge (spec §4).

The effective roles of a query come from the VERIFIED JWT claims, never from
the request body. The body may only narrow that set; the connection allowlist
is the last barrier. Every test here pins one leg of that invariant:
identity and body can only ever restrict, never widen.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from typing import Any

import jwt as pyjwt
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from loom.core.observability.event import LifecycleEvent
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.sql.config import SqlEndpointConfig
from loom.core.sql.service import SqlQueryService
from loom.rest.auth import JwtAuthConfig, JwtAuthMiddleware
from loom.rest.fastapi.sql import bind_sql_endpoints
from tests.unit.core.sql._fakes import (
    FakeSqlExecutor,
    make_connection_config,
    make_sql_config,
)

ROLES_CLAIM = "loom_sql_roles"
ROLE_A = "role_viz_reader"
ROLE_B = "role_viz_sales"
ROLE_FORBIDDEN = "role_admin_evil"

_Scope = dict[str, Any]
_Receive = Callable[[], Awaitable[dict[str, Any]]]
_Send = Callable[[dict[str, Any]], Awaitable[None]]
_ASGIApp = Callable[[_Scope, _Receive, _Send], Awaitable[None]]


class _ClaimsMiddleware:
    """Publishes verified claims exactly like ``JwtAuthMiddleware`` does."""

    def __init__(self, app: _ASGIApp, *, claims: dict[str, Any] | None) -> None:
        self._app = app
        self._claims = claims

    async def __call__(self, scope: _Scope, receive: _Receive, send: _Send) -> None:
        if scope["type"] == "http" and self._claims is not None:
            state: dict[str, Any] = scope.setdefault("state", {})
            state["jwt_claims"] = self._claims
        await self._app(scope, receive, send)


class _RecordingObserver:
    """Captures every lifecycle event emitted through the observability runtime."""

    def __init__(self) -> None:
        self.events: list[LifecycleEvent] = []

    def on_event(self, event: LifecycleEvent) -> None:
        self.events.append(event)


def _endpoint(**overrides: Any) -> SqlEndpointConfig:
    params: dict[str, Any] = {"enabled": True, "auth": "jwt", "roles_claim": ROLES_CLAIM}
    params.update(overrides)
    return SqlEndpointConfig(**params)


def _app(
    executor: FakeSqlExecutor,
    *,
    runtime: ObservabilityRuntime | None = None,
    **conn_overrides: Any,
) -> FastAPI:
    connection = make_connection_config(sql_endpoint=_endpoint(), **conn_overrides)
    config = make_sql_config(analytics=connection)
    app = FastAPI()
    bind_sql_endpoints(
        app,
        service=SqlQueryService(executors={"analytics": executor}, config=config),
        config=config,
        observability_runtime=runtime,
    )
    return app


def _client(
    executor: FakeSqlExecutor,
    *,
    claims: dict[str, Any] | None,
    runtime: ObservabilityRuntime | None = None,
    **conn_overrides: Any,
) -> TestClient:
    app = _app(executor, runtime=runtime, **conn_overrides)
    app.add_middleware(_ClaimsMiddleware, claims=claims)
    return TestClient(app, raise_server_exceptions=False)


def _post(client: TestClient, **body: Any) -> Any:
    payload: dict[str, Any] = {"sql": "SELECT 1"}
    payload.update(body)
    return client.post("/sql/analytics", json=payload)


def _claims(roles: Any, *, subject: str = "user-1") -> dict[str, Any]:
    return {"sub": subject, ROLES_CLAIM: roles}


# ---------------------------------------------------------------------------
# The claim is the only source of authorization
# ---------------------------------------------------------------------------


def test_denies_when_the_request_carries_no_verified_claims() -> None:
    """No ``jwt_claims`` in the ASGI scope means no identity: 403, nothing executed."""
    executor = FakeSqlExecutor()
    response = _post(_client(executor, claims=None))
    assert (response.status_code, executor.calls) == (403, [])


def test_denies_when_the_configured_claim_is_absent() -> None:
    """A verified token without the roles claim authorizes nothing."""
    executor = FakeSqlExecutor()
    response = _post(_client(executor, claims={"sub": "user-1"}))
    assert (response.status_code, executor.calls) == (403, [])


def test_denies_when_the_claim_is_empty() -> None:
    """An empty roles claim is not an implicit 'everything' (fail-closed)."""
    executor = FakeSqlExecutor()
    response = _post(_client(executor, claims=_claims([])))
    assert (response.status_code, executor.calls) == (403, [])


@pytest.mark.parametrize(
    "value",
    [123, True, {"role": ROLE_A}, [ROLE_A, 7], [[ROLE_A]], None],
    ids=["int", "bool", "mapping", "mixed-list", "nested-list", "null"],
)
def test_denies_when_the_claim_is_not_a_string_or_a_list_of_strings(value: Any) -> None:
    """Anything but ``str`` or ``list[str]`` is refused instead of coerced."""
    executor = FakeSqlExecutor()
    response = _post(_client(executor, claims=_claims(value)))
    assert (response.status_code, executor.calls) == (403, [])


def test_denies_with_the_standard_error_body() -> None:
    """Denials reuse the framework error body so callers get code + trace_id."""
    detail = _post(_client(FakeSqlExecutor(), claims=None)).json()["detail"]
    assert (detail["code"], "trace_id" in detail) == ("forbidden", True)


def test_accepts_a_single_string_claim_as_one_role() -> None:
    """A scalar claim value is the one-role form of the same contract."""
    executor = FakeSqlExecutor()
    response = _post(_client(executor, claims=_claims(ROLE_A)))
    assert (response.status_code, executor.calls[0].options.roles) == (200, (ROLE_A,))


# ---------------------------------------------------------------------------
# Intersection with the allowlist — the identity can never widen it
# ---------------------------------------------------------------------------


def test_intersects_the_claim_with_the_connection_allowlist() -> None:
    """A claimed role outside ``allowed_roles`` is dropped, the rest still runs."""
    executor = FakeSqlExecutor()
    response = _post(_client(executor, claims=_claims([ROLE_A, ROLE_FORBIDDEN])))
    assert (response.status_code, executor.calls[0].options.roles) == (200, (ROLE_A,))


def test_denies_when_no_claimed_role_survives_the_allowlist() -> None:
    """An empty intersection is a denial, never a fallback."""
    executor = FakeSqlExecutor()
    response = _post(_client(executor, claims=_claims([ROLE_FORBIDDEN])))
    assert (response.status_code, executor.calls) == (403, [])


def test_default_role_never_replaces_the_identity_intersection() -> None:
    """``default_role`` is not a fallback once roles are bound to the identity."""
    executor = FakeSqlExecutor()
    response = _post(_client(executor, claims=_claims([ROLE_FORBIDDEN]), default_role=ROLE_A))
    assert (response.status_code, executor.calls) == (403, [])


def test_missing_claims_do_not_fall_back_to_the_default_role() -> None:
    """Without verified claims the query never runs with ``default_role`` either."""
    executor = FakeSqlExecutor()
    response = _post(_client(executor, claims=None, default_role=ROLE_A))
    assert (response.status_code, executor.calls) == (403, [])


# ---------------------------------------------------------------------------
# The body may only narrow
# ---------------------------------------------------------------------------


def test_applies_every_authorized_role_when_the_body_asks_for_none() -> None:
    """Without ``roles`` in the body the query runs with the union of the identity's."""
    executor = FakeSqlExecutor()
    response = _post(_client(executor, claims=_claims([ROLE_A, ROLE_B])))
    assert (response.status_code, executor.calls[0].options.roles) == (200, (ROLE_A, ROLE_B))


def test_body_narrows_the_authorized_roles() -> None:
    """A body subset restricts the query to exactly those roles."""
    executor = FakeSqlExecutor()
    client = _client(executor, claims=_claims([ROLE_A, ROLE_B]))
    response = _post(client, roles=[ROLE_B])
    assert (response.status_code, executor.calls[0].options.roles) == (200, (ROLE_B,))


def test_body_cannot_request_a_role_the_identity_does_not_hold() -> None:
    """An allowlisted role absent from the claim is still refused: no widening."""
    executor = FakeSqlExecutor()
    client = _client(executor, claims=_claims([ROLE_A]))
    response = _post(client, roles=[ROLE_B])
    assert (response.status_code, executor.calls) == (403, [])


def test_body_cannot_request_a_role_outside_the_allowlist() -> None:
    """The allowlist stays the last barrier even for a claimed role."""
    executor = FakeSqlExecutor()
    client = _client(executor, claims=_claims([ROLE_A, ROLE_FORBIDDEN]))
    response = _post(client, roles=[ROLE_FORBIDDEN])
    assert (response.status_code, executor.calls) == (403, [])


def test_empty_body_roles_behave_like_an_absent_field() -> None:
    """``roles: []`` narrows to nothing, so it is read as 'no narrowing'."""
    executor = FakeSqlExecutor()
    client = _client(executor, claims=_claims([ROLE_A, ROLE_B]))
    response = _post(client, roles=[])
    assert (response.status_code, executor.calls[0].options.roles) == (200, (ROLE_A, ROLE_B))


def test_singular_role_field_is_rejected_by_the_schema() -> None:
    """The body field is ``roles``; the removed singular ``role`` no longer decides."""
    executor = FakeSqlExecutor()
    client = _client(executor, claims=_claims([ROLE_A, ROLE_B]))
    response = _post(client, role=ROLE_B)
    assert (response.status_code, executor.calls) == (422, [])


# ---------------------------------------------------------------------------
# End to end through the real JWT middleware, and audit trail
# ---------------------------------------------------------------------------


def test_roles_flow_from_a_real_verified_token() -> None:
    """The whole chain: signed token → verified claims → effective roles."""
    executor = FakeSqlExecutor()
    app = _app(executor)
    app.add_middleware(
        JwtAuthMiddleware,
        config=JwtAuthConfig(secret="unit-test-secret", algorithms=("HS256",), audience="loom-api"),
    )
    token = pyjwt.encode(
        {
            "sub": "user-1",
            "aud": "loom-api",
            "exp": int(time.time()) + 3600,
            ROLES_CLAIM: [ROLE_A, ROLE_B],
        },
        "unit-test-secret",
        algorithm="HS256",
    )
    client = TestClient(app, raise_server_exceptions=False)

    response = client.post(
        "/sql/analytics",
        json={"sql": "SELECT 1"},
        headers={"Authorization": f"Bearer {token}"},
    )

    assert (response.status_code, executor.calls[0].options.roles) == (200, (ROLE_A, ROLE_B))


def test_unauthenticated_request_never_reaches_the_endpoint() -> None:
    """Without a token the middleware answers 401 before any role resolution."""
    executor = FakeSqlExecutor()
    app = _app(executor)
    app.add_middleware(
        JwtAuthMiddleware,
        config=JwtAuthConfig(secret="unit-test-secret", algorithms=("HS256",), audience="loom-api"),
    )
    client = TestClient(app, raise_server_exceptions=False)
    response = client.post("/sql/analytics", json={"sql": "SELECT 1"})
    assert (response.status_code, executor.calls) == (401, [])


def test_span_labels_the_effective_roles_and_the_subject() -> None:
    """Auditability: the span states who ran the query and with which privileges."""
    observer = _RecordingObserver()
    client = _client(
        FakeSqlExecutor(),
        claims=_claims([ROLE_A, ROLE_B], subject="user-42"),
        runtime=ObservabilityRuntime([observer]),
    )

    response = _post(client, roles=[ROLE_B])

    assert response.status_code == 200
    start = observer.events[0]
    assert (start.meta["roles"], start.meta["subject"]) == (ROLE_B, "user-42")
