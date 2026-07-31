"""Generic per-connection SQL endpoint mounting and envelope encoding.

Implements the optional REST surface of the SQL subsystem
(``specs/sql_api_clickhouse_spec.md`` §3/§4): one ``POST`` route per
connection that opted in with ``sql_endpoint.enabled`` **and** an explicit
``sql_endpoint.auth`` value (double opt-in, B2). The request body only admits
``{sql, roles?, parameters?, limit?, offset?}`` — backend settings are rejected
by schema — and the response is the single :class:`SqlQueryResult` envelope
encoded in one pass by a module-level ``msgspec`` encoder.

Roles are bound to the authenticated identity: when the connection declares
``sql_endpoint.roles_claim`` the effective roles come from that verified claim
intersected with the allowlist, and the body ``roles`` can only narrow them.

Errors reuse the framework standard body (``code``/``message``/``trace_id``)
through :class:`~loom.rest.errors.HttpErrorMapper`, exactly as the router
runtime does.
"""

from __future__ import annotations

import base64
import logging
from collections.abc import Callable, Coroutine
from ipaddress import IPv4Address, IPv6Address
from typing import Any

import msgspec
from fastapi import FastAPI, HTTPException
from starlette.requests import Request
from starlette.responses import Response

from loom.core.errors import LoomError, RuleViolation
from loom.core.model import LoomFrozenStruct
from loom.core.observability.event import Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.sql.config import SqlConfig, SqlConnectionConfig, SqlEndpointConfig
from loom.core.sql.service import SqlQueryService
from loom.core.tracing import get_trace_id
from loom.rest.errors import ErrorField, HttpErrorMapper
from loom.rest.fastapi._sql_roles import resolve_identity
from loom.rest.fastapi.response import MsgspecJSONResponse

_logger = logging.getLogger(__name__)
_error_mapper = HttpErrorMapper()

# Fixed headroom on top of ``max_sql_bytes`` for the JSON envelope around the
# ``sql`` field (``parameters``, ``role``, key names, quoting). The total body
# cap for a connection is ``max_sql_bytes + _BODY_OVERHEAD_BYTES``; anything
# larger is rejected with 413 before buffering beyond the cap.
_BODY_OVERHEAD_BYTES = 64 * 1024


class _SqlQueryRequest(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Body accepted by the SQL endpoint — never backend settings (spec §3).

    ``roles`` may only narrow the roles the verified identity already holds;
    it never selects a role on its own.
    """

    sql: str
    roles: tuple[str, ...] | None = None
    parameters: dict[str, Any] | None = None
    limit: int | None = None
    offset: int = 0


def _role_exposure_notice(endpoint: SqlEndpointConfig, allowed_role_count: int) -> str:
    """State plainly which roles a caller of this endpoint can obtain.

    ``allowed_roles`` is the ceiling of the connection; the effective roles are
    the ones the verified ``roles_claim`` grants inside that ceiling. Without a
    claim binding, a mounted endpoint is necessarily single-role by config
    (threat model in ``docs/rest/sql.md``).
    """
    if endpoint.roles_claim is None:
        return (
            "the allowlist is empty, so every caller-supplied role is rejected and "
            "queries run only with 'default_role' — one shared role for every caller"
        )
    return (
        f"the effective roles come from the verified {endpoint.roles_claim!r} claim "
        f"intersected with these {allowed_role_count} allowed roles; a caller whose "
        "claim carries none of them is refused"
    )


def _encode_exotic(obj: Any) -> str:
    """Encode backend types msgspec does not handle natively (spec §3 matrix).

    msgspec natively covers datetime/date/UUID/Decimal; this hook adds
    IPv4/IPv6 → str, bytes → base64, and a documented ``str()`` fallback so an
    exotic backend type never produces a bodyless 500.
    """
    if isinstance(obj, (IPv4Address, IPv6Address)):
        return str(obj)
    if isinstance(obj, bytes):
        return base64.b64encode(obj).decode("ascii")
    return str(obj)


_REQUEST_DECODER = msgspec.json.Decoder(_SqlQueryRequest)
_RESULT_ENCODER = msgspec.json.Encoder(enc_hook=_encode_exotic)


class _SqlJSONResponse(MsgspecJSONResponse):
    """Envelope response encoded once by the module-level SQL encoder."""

    def render(self, content: object) -> bytes:
        """Encode *content* to JSON bytes in a single pass."""
        return _RESULT_ENCODER.encode(content)


def _invalid_request(field: str, message: str) -> HTTPException:
    """Build a 422 with the framework standard error body."""
    return _error_mapper.to_http(RuleViolation(field, message))


def _payload_too_large(max_bytes: int) -> HTTPException:
    """Build a 413 with the framework standard error body."""
    message = f"Request body exceeds the maximum accepted size ({max_bytes} bytes)"
    return HTTPException(
        status_code=413,
        detail={
            ErrorField.CODE: "payload_too_large",
            ErrorField.MESSAGE: message,
            ErrorField.TRACE_ID: get_trace_id(),
        },
    )


def _declared_content_length(request: Request) -> int | None:
    """Return the Content-Length header as an int, or ``None`` when unusable."""
    header = request.headers.get("content-length")
    if header is None:
        return None
    try:
        return int(header)
    except ValueError:
        return None


async def _read_body_capped(request: Request, *, max_bytes: int) -> bytes:
    """Read the request body without ever buffering more than *max_bytes*.

    The Content-Length check is only a fast path for honest clients; the
    capped stream read is the authoritative defense — it also covers chunked
    bodies and lying headers, aborting the read as soon as the cap is
    exceeded.

    Raises:
        HTTPException: 413 with the standard error body when the cap is hit.
    """
    declared = _declared_content_length(request)
    if declared is not None and declared > max_bytes:
        raise _payload_too_large(max_bytes)
    received = 0
    chunks: list[bytes] = []
    async for chunk in request.stream():
        received += len(chunk)
        if received > max_bytes:
            raise _payload_too_large(max_bytes)
        chunks.append(chunk)
    return b"".join(chunks)


def _decode_request(body: bytes, *, max_sql_bytes: int) -> _SqlQueryRequest:
    """Decode and validate the request body at the input edge."""
    try:
        query = _REQUEST_DECODER.decode(body)
    except msgspec.DecodeError as exc:
        raise _invalid_request("body", str(exc)) from exc
    if len(query.sql.encode("utf-8")) > max_sql_bytes:
        raise _invalid_request("sql", f"SQL statement exceeds max_sql_bytes ({max_sql_bytes})")
    return query


def _unexpected_error_response() -> MsgspecJSONResponse:
    """Replicate the router runtime generic 500 body without leaking internals."""
    return MsgspecJSONResponse(
        status_code=500,
        content={
            ErrorField.CODE: "internal_error",
            ErrorField.MESSAGE: "An unexpected error occurred",
            ErrorField.TRACE_ID: get_trace_id() or "",
        },
    )


def _make_sql_handler(
    service: SqlQueryService,
    name: str,
    connection: SqlConnectionConfig,
    *,
    path: str,
    observability_runtime: ObservabilityRuntime,
) -> Callable[[Request], Coroutine[Any, Any, Response]]:
    """Build the async handler serving SQL queries for one connection."""
    handler_name = f"execute_sql_{name}"
    max_body_bytes = connection.max_sql_bytes + _BODY_OVERHEAD_BYTES
    allowed_roles = frozenset(connection.allowed_roles)
    roles_claim = connection.sql_endpoint.roles_claim

    async def _handler(request: Request) -> Response:
        try:
            body = await _read_body_capped(request, max_bytes=max_body_bytes)
            query = _decode_request(body, max_sql_bytes=connection.max_sql_bytes)
            # Resolved before the span so it can label who runs the query with
            # which privileges — the audit trail the endpoint is judged on.
            identity = resolve_identity(
                request.scope,
                connection=name,
                roles_claim=roles_claim,
                allowed_roles=allowed_roles,
                requested_roles=query.roles,
            )
            with observability_runtime.span(
                Scope.USE_CASE,
                handler_name,
                trace_id=get_trace_id(),
                route=path,
                method="POST",
                status_code=200,
                read_only=connection.readonly,
                roles=",".join(identity.roles),
                subject=identity.subject,
            ):
                result = await service.execute(
                    query.sql,
                    connection=name,
                    roles=identity.roles,
                    parameters=query.parameters,
                    limit=query.limit,
                    offset=query.offset,
                )
            return _SqlJSONResponse(content=result)
        except HTTPException:
            raise
        except LoomError as exc:
            raise _error_mapper.to_http(exc) from exc
        except Exception:
            _logger.exception("Unhandled error in SQL endpoint for connection %r", name)
            return _unexpected_error_response()

    _handler.__name__ = handler_name
    return _handler


def _mount_endpoint(
    app: FastAPI,
    *,
    service: SqlQueryService,
    name: str,
    connection: SqlConnectionConfig,
    observability_runtime: ObservabilityRuntime,
) -> None:
    """Register the POST route for *name* and emit the startup WARNING (§4)."""
    endpoint = connection.sql_endpoint
    path = endpoint.path or f"/sql/{name}"
    app.add_api_route(
        path,
        _make_sql_handler(
            service, name, connection, path=path, observability_runtime=observability_runtime
        ),
        methods=["POST"],
        include_in_schema=endpoint.include_in_schema,
    )
    _logger.warning(
        "SQL endpoint mounted: path=%s connection=%s readonly=%s auth=%s allowed_roles=%d. "
        "'auth' only authenticates the caller; the roles it may use come from the "
        "identity binding: %s",
        path,
        name,
        connection.readonly,
        endpoint.auth,
        len(connection.allowed_roles),
        _role_exposure_notice(endpoint, len(connection.allowed_roles)),
    )


def bind_sql_endpoints(
    app: FastAPI,
    *,
    service: SqlQueryService,
    config: SqlConfig,
    observability_runtime: ObservabilityRuntime | None = None,
) -> None:
    """Mount one generic SQL endpoint per opted-in connection.

    Only connections with ``sql_endpoint.enabled`` and an explicit
    ``sql_endpoint.auth`` mount a route (double opt-in, B2 resolved); every
    mounted endpoint is announced with a WARNING carrying its
    security-relevant state. Connections without endpoint expose no HTTP
    surface at all.

    Args:
        app: FastAPI application to mount the routes on.
        service: Policy-applying SQL query service shared by every endpoint.
        config: Parsed ``sql:`` section with the named connections.
        observability_runtime: Runtime emitting one span per request, with the
            same labels the router runtime uses. ``None`` falls back to a
            no-op runtime.

    Example::

        service = SqlQueryService(executors=executors, config=sql_config)
        bind_sql_endpoints(app, service=service, config=sql_config)
    """
    runtime = (
        observability_runtime if observability_runtime is not None else ObservabilityRuntime.noop()
    )
    for name, connection in config.connections.items():
        endpoint = connection.sql_endpoint
        if not endpoint.enabled or endpoint.auth is None:
            continue
        _mount_endpoint(
            app, service=service, name=name, connection=connection, observability_runtime=runtime
        )
