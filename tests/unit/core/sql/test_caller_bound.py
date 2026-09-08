"""Identity-bound SQL execution tests for ``CallerBoundSql``.

The collaborator exists so a use case cannot query with roles its caller does
not hold: the roles come from the verified identity and from nowhere else.
"""

from __future__ import annotations

import inspect

import pytest

from loom.core.config.errors import ConfigError
from loom.core.identity import ANONYMOUS, Identity
from loom.core.sql.abc import RolesNotBoundError, UnknownConnectionError
from loom.core.sql.caller_bound import CallerBoundSql
from loom.core.sql.config import SqlConfig, SqlConnectionConfig
from loom.core.sql.service import NullSqlQueryService, SqlQueryService
from tests.unit.core.sql._fakes import FakeSqlExecutor, make_connection_config, make_sql_config


def _caller_bound(executor: FakeSqlExecutor, connection: SqlConnectionConfig) -> CallerBoundSql:
    """Build a ``CallerBoundSql`` over a fake 'analytics' connection."""
    config = make_sql_config(analytics=connection)
    service = SqlQueryService(executors={"analytics": executor}, config=config)
    return CallerBoundSql(service, config)


def _identity(*roles: str) -> Identity:
    return Identity(subject="user-1", roles=roles, mechanism="jwt")


async def test_runs_with_the_roles_the_identity_holds(fake_executor: FakeSqlExecutor) -> None:
    """The effective roles are the ones carried by the verified identity."""
    sql = _caller_bound(fake_executor, make_connection_config())
    await sql.execute("SELECT 1", connection="analytics", identity=_identity("role_viz_sales"))
    assert fake_executor.calls[0].options.roles == ("role_viz_sales",)


async def test_keeps_only_the_identity_roles_that_are_allowlisted(
    fake_executor: FakeSqlExecutor,
) -> None:
    """A role the identity holds but the connection does not allowlist is dropped."""
    sql = _caller_bound(fake_executor, make_connection_config())
    await sql.execute(
        "SELECT 1",
        connection="analytics",
        identity=_identity("role_viz_sales", "role_intruder"),
    )
    assert fake_executor.calls[0].options.roles == ("role_viz_sales",)


async def test_rejects_an_identity_holding_no_allowlisted_role_before_the_backend(
    fake_executor: FakeSqlExecutor,
) -> None:
    """The caller's own entitlements decide, and the refusal precedes execution."""
    sql = _caller_bound(fake_executor, make_connection_config())
    with pytest.raises(RolesNotBoundError):
        await sql.execute("SELECT 1", connection="analytics", identity=_identity("role_intruder"))
    assert fake_executor.calls == []


async def test_rejects_an_identity_carrying_no_role(fake_executor: FakeSqlExecutor) -> None:
    """An authenticated caller without roles cannot borrow any."""
    sql = _caller_bound(fake_executor, make_connection_config())
    with pytest.raises(RolesNotBoundError):
        await sql.execute("SELECT 1", connection="analytics", identity=_identity())
    assert fake_executor.calls == []


async def test_rejects_an_anonymous_caller(fake_executor: FakeSqlExecutor) -> None:
    """No verified identity means no query, whatever the connection allows."""
    sql = _caller_bound(fake_executor, make_connection_config())
    with pytest.raises(RolesNotBoundError):
        await sql.execute("SELECT 1", connection="analytics", identity=ANONYMOUS)
    assert fake_executor.calls == []


async def test_never_falls_back_to_the_shared_default_role(
    fake_executor: FakeSqlExecutor,
) -> None:
    """A rejected caller is refused, not silently run as the connection's shared role."""
    connection = make_connection_config(default_role="role_viz_reader")
    sql = _caller_bound(fake_executor, connection)
    with pytest.raises(RolesNotBoundError):
        await sql.execute("SELECT 1", connection="analytics", identity=_identity("role_intruder"))
    assert fake_executor.calls == []


async def test_refuses_a_connection_whose_allowlist_is_empty(
    fake_executor: FakeSqlExecutor,
) -> None:
    """A single-role connection has nothing to bind to an identity, so it is refused."""
    connection = make_connection_config(allowed_roles=(), default_role="role_viz_reader")
    sql = _caller_bound(fake_executor, connection)
    with pytest.raises(RolesNotBoundError):
        await sql.execute("SELECT 1", connection="analytics", identity=_identity("role_viz_reader"))
    assert fake_executor.calls == []


def test_execute_accepts_no_roles_argument() -> None:
    """No parameter may let the calling code choose the roles — that is the whole point."""
    parameters = inspect.signature(CallerBoundSql.execute).parameters
    assert "roles" not in parameters
    assert parameters["identity"].default is inspect.Parameter.empty


async def test_forwards_parameters_limit_and_offset(fake_executor: FakeSqlExecutor) -> None:
    """Everything but the roles behaves exactly as in the unbound service."""
    sql = _caller_bound(fake_executor, make_connection_config(max_limit=100))
    await sql.execute(
        "SELECT 1",
        connection="analytics",
        identity=_identity("role_viz_sales"),
        parameters={"id": 7},
        limit=500,
        offset=20,
    )
    call = fake_executor.calls[0]
    assert call.parameters == {"id": 7}
    assert call.options.limit == 100
    assert call.options.offset == 20


async def test_unknown_connection_is_reported_as_such(fake_executor: FakeSqlExecutor) -> None:
    """An unconfigured connection name fails with the domain error of the pillar."""
    sql = _caller_bound(fake_executor, make_connection_config())
    with pytest.raises(UnknownConnectionError):
        await sql.execute("SELECT 1", connection="unknown", identity=_identity("role_viz_sales"))
    assert fake_executor.calls == []


async def test_without_a_sql_section_the_actionable_config_error_surfaces() -> None:
    """Wrapping the null service keeps its actionable error instead of hiding it."""
    sql = CallerBoundSql(NullSqlQueryService(), SqlConfig(connections={}))
    with pytest.raises(ConfigError, match="sql"):
        await sql.execute("SELECT 1", connection="analytics", identity=_identity("role_viz_sales"))
