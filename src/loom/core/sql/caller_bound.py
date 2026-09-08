"""Caller-bound SQL execution: the roles come from the verified identity.

:class:`~loom.core.sql.service.SqlQueryService` takes the roles as an argument,
so the code that writes the call chooses them. That is the right shape for
system work with no caller, and the wrong one for work done *on behalf of* a
caller: within the connection allowlist, a use case could then query with roles
its caller does not hold.

This module gives that second case its own collaborator. It resolves the
effective roles through :func:`~loom.core.sql.roles.resolve_query_roles`, the
same function the agent capability and the REST endpoint use, and exposes no
way to pass roles in.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from loom.core.identity import Identity
from loom.core.sql.abc import RolesNotBoundError, SqlQueryResult
from loom.core.sql.config import SqlConfig
from loom.core.sql.roles import resolve_query_roles
from loom.core.sql.service import SqlQueryService


class CallerBoundSql:
    """Executes SQL with the roles of the verified caller, and only those.

    Every query is bound to an :class:`~loom.core.identity.identity.Identity`:
    the effective roles are the intersection of the roles that identity holds
    with the connection allowlist. There is deliberately no ``roles``
    parameter — accepting one would reintroduce the very gap this collaborator
    closes, since the caller's entitlements would stop being what decides.

    A connection with an empty ``allowed_roles`` cannot be queried this way: its
    only role is the shared ``default_role``, which is not derived from anyone,
    so :class:`~loom.core.sql.abc.errors.RolesNotBoundError` is raised. Use
    :class:`~loom.core.sql.service.SqlQueryService` for that unbound work.

    Args:
        service: Underlying query service applying the connection policy.
        config: Parsed ``sql:`` section holding the per-connection allowlists.

    Example::

        class ListSales:
            def __init__(self, sql: CallerBoundSql) -> None:
                self._sql = sql

            async def execute(self, identity: Identity) -> SqlQueryResult:
                return await self._sql.execute(
                    "SELECT * FROM sales", connection="analytics", identity=identity
                )
    """

    def __init__(self, service: SqlQueryService, config: SqlConfig) -> None:
        self._service = service
        # The config is frozen, so the allowlists are precomputed once, as the
        # service itself does, for O(1) role membership checks per query.
        self._allowed_roles: dict[str, frozenset[str]] = {
            name: frozenset(connection.allowed_roles)
            for name, connection in config.connections.items()
        }

    async def execute(
        self,
        sql: str,
        *,
        connection: str,
        identity: Identity,
        parameters: Mapping[str, Any] | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> SqlQueryResult:
        """Execute *sql* on *connection* with the roles *identity* holds.

        Args:
            sql: SQL statement with native parameter placeholders.
            connection: Name of the configured connection to use.
            identity: Verified caller published by the authentication
                mechanism; the sole source of the effective roles.
            parameters: Values bound server-side by the backend.
            limit: Requested row limit; clamped to the connection ``max_limit``
                and defaulted to ``default_limit`` when absent.
            offset: Number of rows to skip.

        Returns:
            The standard tabular result envelope from the executor.

        Raises:
            UnknownConnectionError: When *connection* is not configured.
            RolesNotBoundError: When the identity is anonymous, holds no role,
                or holds none that the connection allowlists.
            ConfigError: When the application has no ``sql:`` section.
        """
        allowed_roles = self._allowed_roles.get(connection)
        if allowed_roles is None:
            # Unknown to this collaborator means unknown to the service too, so
            # it phrases the failure: an actionable ConfigError when no 'sql:'
            # section exists, UnknownConnectionError otherwise. Neither reaches
            # an executor — the service validates the connection first.
            return await self._service.execute(
                sql, connection=connection, parameters=parameters, limit=limit, offset=offset
            )
        roles = self._bound_roles(identity, connection, allowed_roles)
        return await self._service.execute(
            sql,
            connection=connection,
            roles=roles,
            parameters=parameters,
            limit=limit,
            offset=offset,
        )

    def _bound_roles(
        self,
        identity: Identity,
        connection: str,
        allowed_roles: frozenset[str],
    ) -> tuple[str, ...]:
        """Derive the caller's roles; the shared ``default_role`` is unreachable.

        ``roles_bound`` is hard-coded ``True``, because binding to the identity
        is what this collaborator is, and the result is re-checked for
        emptiness: ``()`` would reach ``SqlQueryService.execute`` and fall back
        to the connection's shared role (FR-043a).
        """
        roles = resolve_query_roles(
            identity,
            connection=connection,
            roles_bound=True,
            allowed_roles=allowed_roles,
            requested_roles=None,
        )
        if not roles:
            raise RolesNotBoundError(connection)
        return roles
