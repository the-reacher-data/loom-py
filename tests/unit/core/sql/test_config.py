"""Parsing and fail-fast validation tests for the ``sql:`` section (spec §3/§7.1)."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.config import ConfigContext, ConfigKey
from loom.core.config.errors import ConfigError
from loom.core.sql.config import SqlConfig


def _minimal(**overrides: Any) -> dict[str, Any]:
    """Minimal valid connection per the spec (backend + url)."""
    base: dict[str, Any] = {"backend": "clickhouse", "url": "http://localhost:8123"}
    base.update(overrides)
    return base


def _parse(connection: dict[str, Any]) -> SqlConfig:
    """Parse a single 'analytics' connection via ConfigContext + ConfigKey.SQL."""
    ctx = ConfigContext.from_dict({"sql": {"connections": {"analytics": connection}}})
    return ctx.section(ConfigKey.SQL, SqlConfig)


def test_config_key_sql_points_at_the_sql_section() -> None:
    """``ConfigKey.SQL`` exists and resolves to the 'sql' YAML path."""
    assert str(ConfigKey.SQL) == "sql"


def test_parses_defaults_when_connection_is_minimal() -> None:
    """A connection with only backend+url adopts every default from spec §3."""
    conn = _parse(_minimal()).connections["analytics"]
    assert (
        conn.readonly,
        conn.default_limit,
        conn.max_limit,
        conn.max_execution_time,
        conn.max_sql_bytes,
        conn.connect_timeout,
        conn.send_receive_timeout,
        conn.executor_threads,
        conn.pool_size,
    ) == (True, 1000, 10000, 30, 262144, 10, 60, None, None)


def test_endpoint_is_disabled_when_not_declared() -> None:
    """``sql_endpoint.enabled`` is opt-in: default False (double opt-in of §4)."""
    conn = _parse(_minimal()).connections["analytics"]
    assert conn.sql_endpoint.enabled is False


def test_extra_settings_are_empty_when_not_declared() -> None:
    """``settings:`` defaults to an empty mapping (no implicit CH settings)."""
    conn = _parse(_minimal()).connections["analytics"]
    assert dict(conn.settings) == {}


def test_parses_allowed_roles_and_default_role_when_declared() -> None:
    """The allowlist and default_role from YAML arrive typed in the config."""
    conn = _parse(
        _minimal(
            allowed_roles=["role_viz_reader", "role_viz_sales"],
            default_role="role_viz_reader",
        )
    ).connections["analytics"]
    assert (tuple(conn.allowed_roles), conn.default_role) == (
        ("role_viz_reader", "role_viz_sales"),
        "role_viz_reader",
    )


def test_parses_multiple_connections_when_more_than_one() -> None:
    """Every entry under ``connections`` yields its own named connection."""
    ctx = ConfigContext.from_dict(
        {"sql": {"connections": {"analytics": _minimal(), "reporting": _minimal()}}}
    )
    config = ctx.section(ConfigKey.SQL, SqlConfig)
    assert set(config.connections) == {"analytics", "reporting"}


def test_fails_when_url_is_missing() -> None:
    """``url`` is mandatory: its absence aborts the parse with ConfigError."""
    with pytest.raises(ConfigError):
        _parse({"backend": "clickhouse"})


def test_fails_when_an_allowlist_role_has_invalid_format() -> None:
    """Roles outside ``^[A-Za-z0-9_]+$`` are rejected fail-fast (anti-injection)."""
    with pytest.raises(ConfigError):
        _parse(_minimal(allowed_roles=["role-bad!"]))


def test_fails_when_default_role_has_invalid_format() -> None:
    """``default_role`` follows the same format validation as the allowlist."""
    with pytest.raises(ConfigError):
        _parse(_minimal(default_role="bad role;drop"))


def test_fails_when_backend_is_unknown() -> None:
    """Only ``clickhouse`` is supported: any other backend => ConfigError."""
    with pytest.raises(ConfigError):
        _parse(_minimal(backend="postgres"))


def test_fails_when_default_limit_exceeds_max_limit() -> None:
    """``default_limit <= max_limit`` is validated at parse time, not at runtime."""
    with pytest.raises(ConfigError):
        _parse(_minimal(default_limit=100, max_limit=10))


def test_fails_when_endpoint_enabled_without_default_role_or_allowlist() -> None:
    """``enabled: true`` without any role cannot mount an endpoint (fail-closed)."""
    with pytest.raises(ConfigError):
        _parse(_minimal(sql_endpoint={"enabled": True, "auth": "external"}))


def test_repr_redacts_the_dsn_credentials() -> None:
    """``repr`` never exposes the DSN password (docs promise redaction)."""
    conn = _parse(
        _minimal(url="clickhouse://loom_user:s3cr3t_pw@ch.internal:8123/analytics")
    ).connections["analytics"]
    rendered = repr(conn)
    assert ("s3cr3t_pw" in rendered, "://***@ch.internal" in rendered) == (False, True)


def test_fails_when_auth_has_an_unknown_value() -> None:
    """``auth`` only admits 'jwt' or 'external'; any other value => ConfigError."""
    with pytest.raises(ConfigError):
        _parse(
            _minimal(
                allowed_roles=["role_viz_reader"],
                default_role="role_viz_reader",
                sql_endpoint={"enabled": True, "auth": "basic"},
            )
        )
