"""Typed configuration for the backend-agnostic ``sql:`` section.

Parsed via ``ctx.section(ConfigKey.SQL, SqlConfig)``. Validation is fail-fast:
invalid values abort the parse and surface as
:class:`~loom.core.config.errors.ConfigError` through the config loader.
"""

from __future__ import annotations

import re
from typing import Any, Literal

import msgspec

from loom.core.model import LoomFrozenStruct

# re.ASCII keeps the exact [A-Za-z0-9_] charset: without it \w would also
# accept Unicode word characters, widening this security allowlist.
_ROLE_PATTERN = re.compile(r"^\w+$", re.ASCII)
_URL_CREDENTIALS_RE = re.compile(r"://[^@]+@")


def _redact_url(url: str) -> str:
    return _URL_CREDENTIALS_RE.sub("://***@", url)


class SqlEndpointConfig(LoomFrozenStruct, frozen=True, kw_only=True):
    """Opt-in REST endpoint settings for a SQL connection.

    Attributes:
        enabled: Whether to mount the generic ``POST /sql/{name}`` endpoint.
            Defaults to ``False`` (double opt-in).
        auth: Mandatory when ``enabled``: ``"jwt"`` (requires the framework
            JWT middleware configured) or ``"external"`` (explicit
            acknowledgement that the operator provides auth).
        path: Mount path override. Defaults to ``/sql/{name}`` when ``None``.
        include_in_schema: Whether the endpoint appears in the OpenAPI schema.
    """

    enabled: bool = False
    auth: Literal["jwt", "external"] | None = None
    path: str | None = None
    include_in_schema: bool = False


class SqlConnectionConfig(LoomFrozenStruct, frozen=True, kw_only=True):
    """Named SQL connection with role policy, limits and driver tuning.

    Attributes:
        backend: Backend identifier. Only ``"clickhouse"`` is supported; any
            other value fails the parse.
        url: Backend DSN. Canonically injected via environment or secret
            resolver, never inline.
        allowed_roles: Allowlist of roles a caller may request. Empty means
            every caller-provided role is rejected (fail-closed).
        default_role: Role applied when the request carries none. Without it,
            a request without role is refused.
        readonly: Whether queries run in read-only mode. Defaults to ``True``.
        default_limit: Row limit applied when the request brings none.
        max_limit: Hard cap for any requested limit.
        max_execution_time: Per-query execution timeout in seconds.
        max_sql_bytes: Maximum accepted SQL statement size in bytes.
        connect_timeout: Driver connect timeout in seconds.
        send_receive_timeout: Driver send/receive timeout in seconds.
        executor_threads: Async client thread pool size (driver default if
            ``None``).
        pool_size: HTTP connection pool size (driver default if ``None``).
        settings: Extra backend settings handed to the executor at
            construction. They can never override the policy.
        sql_endpoint: Optional REST endpoint settings for this connection.

    Raises:
        ValueError: On invalid role format, ``default_limit`` above
            ``max_limit``, or an enabled endpoint without role/auth. Surfaced
            as ``ConfigError`` when parsed through the config loader.
    """

    backend: Literal["clickhouse"]
    url: str
    allowed_roles: tuple[str, ...] = ()
    default_role: str | None = None
    readonly: bool = True
    default_limit: int = 1000
    max_limit: int = 10000
    max_execution_time: int = 30
    max_sql_bytes: int = 262144
    connect_timeout: int = 10
    send_receive_timeout: int = 60
    executor_threads: int | None = None
    pool_size: int | None = None
    settings: dict[str, Any] = msgspec.field(default_factory=dict)
    sql_endpoint: SqlEndpointConfig = msgspec.field(default_factory=SqlEndpointConfig)

    def __post_init__(self) -> None:
        _validate_roles(self.allowed_roles, self.default_role)
        _validate_limits(self.default_limit, self.max_limit)
        _validate_endpoint(self.sql_endpoint, self.allowed_roles, self.default_role)

    def __repr__(self) -> str:
        return (
            f"SqlConnectionConfig(backend={self.backend!r},"
            f" url={_redact_url(self.url)!r},"
            f" allowed_roles={self.allowed_roles!r},"
            f" default_role={self.default_role!r},"
            f" readonly={self.readonly!r},"
            f" sql_endpoint={self.sql_endpoint!r})"
        )


class SqlConfig(LoomFrozenStruct, frozen=True, kw_only=True):
    """Root of the ``sql:`` config section.

    Attributes:
        connections: Named SQL connections available to the query service.
    """

    connections: dict[str, SqlConnectionConfig]


def _validate_roles(allowed_roles: tuple[str, ...], default_role: str | None) -> None:
    for role in allowed_roles:
        _validate_role_format(role)
    if default_role is not None:
        _validate_role_format(default_role)


def _validate_role_format(role: str) -> None:
    if not _ROLE_PATTERN.fullmatch(role):
        raise ValueError(
            f"Invalid SQL role {role!r}: roles must contain only "
            "ASCII letters, digits or underscores"
        )


def _validate_limits(default_limit: int, max_limit: int) -> None:
    if default_limit > max_limit:
        raise ValueError(f"default_limit ({default_limit}) must not exceed max_limit ({max_limit})")


def _validate_endpoint(
    endpoint: SqlEndpointConfig,
    allowed_roles: tuple[str, ...],
    default_role: str | None,
) -> None:
    if not endpoint.enabled:
        return
    if not allowed_roles and default_role is None:
        raise ValueError(
            "sql_endpoint.enabled requires a 'default_role' or a non-empty "
            "'allowed_roles' on the connection"
        )
