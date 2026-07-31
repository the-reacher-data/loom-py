"""Config-driven auth gate tests for the SQL endpoint in ``create_app`` (spec §4/§7.11).

These tests drive :func:`loom.rest.fastapi.auto.create_app` with temporary YAML
configs plus a minimal discoverable app package, mirroring the structure used by
``tests/integration/celery_bootstrap``. ``create_app`` must not open any network
connection: the ClickHouse registry is only entered during the app lifespan,
which these tests never start.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi import FastAPI

from loom.core.config.errors import ConfigError
from loom.rest.auth import JwtAuthMiddleware
from loom.rest.fastapi.auto import create_app

_APP_MODULE = "loom_sqlgate_fixture_app"

_APP_SOURCE = '''\
"""Minimal discoverable app used by the SQL endpoint auth-gate tests."""

from __future__ import annotations

from typing import Any

from loom.core.model import BaseModel, ColumnField
from loom.core.use_case.use_case import UseCase
from loom.rest.model import RestInterface, RestRoute


class SqlGateRecord(BaseModel):
    __tablename__ = "sqlgate_records_fixture"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=50)


class SqlGatePingUseCase(UseCase[SqlGateRecord, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "pong"


class SqlGatePingInterface(RestInterface[str]):
    # The prefix must NOT start with "/sql": the tests assert that no route
    # under "/sql" exists when the endpoint does not mount.
    prefix = "/gate-ping"
    routes = (RestRoute(use_case=SqlGatePingUseCase, method="GET", path="/"),)
'''


def _sql_section(endpoint: dict[str, Any], **conn_overrides: Any) -> dict[str, Any]:
    """SQL section with a single 'analytics' connection and the given endpoint."""
    connection: dict[str, Any] = {
        "backend": "clickhouse",
        "url": "http://localhost:8123",
        "allowed_roles": ["role_viz_reader"],
        "default_role": "role_viz_reader",
        "sql_endpoint": endpoint,
    }
    connection.update(conn_overrides)
    return {"connections": {"analytics": connection}}


def _write_project(
    tmp_path: Path,
    *,
    sql_section: dict[str, Any] | None,
    jwt_section: dict[str, Any] | None = None,
) -> str:
    """Write the fixture app module plus a YAML config; return the config path."""
    (tmp_path / f"{_APP_MODULE}.py").write_text(_APP_SOURCE, encoding="utf-8")
    config: dict[str, Any] = {
        "app": {
            "name": "sqlgate-demo",
            "code_path": ".",
            "discovery": {
                "mode": "interfaces",
                "interfaces": {"modules": [_APP_MODULE], "warn_recommended": False},
            },
        },
        "database": {"url": "sqlite+aiosqlite:///"},
    }
    if jwt_section is not None:
        config["app"]["rest"] = {"auth": {"jwt": jwt_section}}
    if sql_section is not None:
        config["sql"] = sql_section
    config_path = tmp_path / "app.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return str(config_path)


def _route_paths(app: FastAPI) -> set[str]:
    return {str(getattr(route, "path", "")) for route in app.routes}


_JWT_SECTION: dict[str, Any] = {"secret": "unit-test-secret", "algorithms": ["HS256"]}


def test_auth_jwt_without_jwt_section_fails_at_startup(tmp_path: Path) -> None:
    """``auth: jwt`` demands ``app.rest.auth.jwt``: its absence aborts create_app."""
    config_path = _write_project(
        tmp_path, sql_section=_sql_section({"enabled": True, "auth": "jwt"})
    )
    with pytest.raises(ConfigError, match="jwt"):
        create_app(config_path)


def test_auth_external_mounts_the_endpoint(tmp_path: Path) -> None:
    """``auth: external`` mounts the route (operator acknowledges external auth)."""
    config_path = _write_project(
        tmp_path, sql_section=_sql_section({"enabled": True, "auth": "external"})
    )
    app = create_app(config_path)
    assert "/sql/analytics" in _route_paths(app)


def test_auth_external_warns_at_startup(tmp_path: Path) -> None:
    """Every mounted SQL endpoint emits a startup warning naming its path."""
    config_path = _write_project(
        tmp_path, sql_section=_sql_section({"enabled": True, "auth": "external"})
    )
    with pytest.warns(UserWarning, match="/sql/analytics"):
        create_app(config_path)


def test_multi_role_endpoint_warns_about_missing_privilege_separation(tmp_path: Path) -> None:
    """The startup warning must state that any caller may pick any allowlisted role.

    ``allowed_roles`` is a shared ceiling for the connection, not a per-caller
    permission, so the warning may never read as if authentication restricted
    the requested role (threat model in ``docs/rest/sql.md``).
    """
    config_path = _write_project(
        tmp_path,
        sql_section=_sql_section(
            {"enabled": True, "auth": "jwt"},
            allowed_roles=["role_viz_reader", "role_viz_sales"],
        ),
        jwt_section=_JWT_SECTION,
    )
    with pytest.warns(UserWarning, match="NO privilege separation per identity"):
        create_app(config_path)


def test_enabled_endpoint_without_auth_field_does_not_mount(tmp_path: Path) -> None:
    """``enabled: true`` without ``auth`` boots the app but mounts nothing (B2)."""
    config_path = _write_project(tmp_path, sql_section=_sql_section({"enabled": True}))
    app = create_app(config_path)
    assert not any(path.startswith("/sql") for path in _route_paths(app))


def test_auth_jwt_with_jwt_section_mounts_the_endpoint(tmp_path: Path) -> None:
    """``auth: jwt`` plus a configured ``app.rest.auth.jwt`` mounts the route."""
    config_path = _write_project(
        tmp_path,
        sql_section=_sql_section({"enabled": True, "auth": "jwt"}),
        jwt_section=_JWT_SECTION,
    )
    app = create_app(config_path)
    assert "/sql/analytics" in _route_paths(app)


def test_jwt_section_mounts_the_jwt_middleware(tmp_path: Path) -> None:
    """A present ``app.rest.auth.jwt`` section wires JwtAuthMiddleware into the app."""
    config_path = _write_project(
        tmp_path,
        sql_section=_sql_section({"enabled": True, "auth": "jwt"}),
        jwt_section=_JWT_SECTION,
    )
    app = create_app(config_path)
    assert any(m.cls is JwtAuthMiddleware for m in app.user_middleware)


def test_non_readonly_enabled_endpoint_warns_at_startup(tmp_path: Path) -> None:
    """``readonly: false`` plus an enabled endpoint triggers a startup warning."""
    config_path = _write_project(
        tmp_path,
        sql_section=_sql_section({"enabled": True, "auth": "external"}, readonly=False),
    )
    with pytest.warns(UserWarning, match="readonly"):
        create_app(config_path)


def test_without_sql_section_no_sql_routes_are_mounted(tmp_path: Path) -> None:
    """Absent ``sql:`` section means zero changes: the app boots with no /sql routes."""
    config_path = _write_project(tmp_path, sql_section=None)
    app = create_app(config_path)
    assert not any(path.startswith("/sql") for path in _route_paths(app))
