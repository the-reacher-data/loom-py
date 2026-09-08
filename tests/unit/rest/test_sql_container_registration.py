"""Container wiring of the SQL collaborators registered by ``create_app``."""

from __future__ import annotations

import pytest

from loom.core.di.container import LoomContainer
from loom.core.identity import Identity
from loom.core.sql import CallerBoundSql, NullSqlQueryService, SqlQueryService
from loom.core.sql.abc import RolesNotBoundError
from loom.rest.fastapi.auto import _register_sql_collaborators, _SqlWiring
from tests.unit.core.sql._fakes import FakeSqlExecutor, make_connection_config, make_sql_config


def _wiring() -> _SqlWiring:
    config = make_sql_config(analytics=make_connection_config())
    service = SqlQueryService(executors={"analytics": FakeSqlExecutor()}, config=config)
    return _SqlWiring(config=config, registry=None, service=service)


def test_registers_both_the_unbound_and_the_caller_bound_collaborators() -> None:
    """A use case may inject either path, so both must be resolvable."""
    container = LoomContainer()
    _register_sql_collaborators(container, _wiring())
    assert isinstance(container.resolve(SqlQueryService), SqlQueryService)
    assert isinstance(container.resolve(CallerBoundSql), CallerBoundSql)


def test_registers_the_caller_bound_collaborator_without_a_sql_section() -> None:
    """Resolution never depends on the config being present (M5)."""
    container = LoomContainer()
    _register_sql_collaborators(
        container, _SqlWiring(config=None, registry=None, service=NullSqlQueryService())
    )
    assert isinstance(container.resolve(CallerBoundSql), CallerBoundSql)


async def test_the_registered_collaborator_binds_the_roles_to_the_identity() -> None:
    """The instance in the container is the bound one, not a passthrough."""
    container = LoomContainer()
    _register_sql_collaborators(container, _wiring())
    sql = container.resolve(CallerBoundSql)
    with pytest.raises(RolesNotBoundError):
        await sql.execute(
            "SELECT 1",
            connection="analytics",
            identity=Identity(subject="user-1", roles=("role_intruder",), mechanism="jwt"),
        )
