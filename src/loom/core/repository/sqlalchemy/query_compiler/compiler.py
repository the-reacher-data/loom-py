"""QuerySpecCompiler — orchestrates filter, ordering, and pagination compilation."""

from __future__ import annotations

from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.core.repository.abc.cursor import Cursor, decode_cursor
from loom.core.repository.abc.errors import UnsupportedQuery
from loom.core.repository.abc.query import (
    PaginationMode,
    QuerySpec,
)
from loom.core.repository.sqlalchemy.query_compiler.cursor import (
    compile_cursor_predicate,
    extract_next_cursor,
)
from loom.core.repository.sqlalchemy.query_compiler.filters import compile_filter_group
from loom.core.repository.sqlalchemy.query_compiler.ordering import compile_order_by


class QuerySpecCompiler:
    """Compiles a :class:`~loom.core.repository.abc.query.QuerySpec` into
    SQLAlchemy statements and executes them.

    Designed to be instantiated once per repository mixin call — it holds
    no mutable state across invocations.

    Args:
        sa_model: SQLAlchemy mapped model class for the target entity.
        id_column: Primary-key column attribute used for default ordering.
        allowed_fields: Permitted filter field names.  Empty frozenset
            allows all fields.
        backend: Backend name stamped on and required from cursor tokens.
        model_name: Qualified model name reported in query errors.

    Example::

        compiler = QuerySpecCompiler(
            ProductSA, ProductSA.id, frozenset(), backend="sqlalchemy", model_name="Product"
        )
        result = await compiler.execute(session, query, profile_options)
    """

    def __init__(
        self,
        sa_model: type[Any],
        id_column: Any,
        allowed_fields: frozenset[str],
        *,
        backend: str,
        model_name: str,
    ) -> None:
        self._sa_model = sa_model
        self._id_column = id_column
        self._allowed_fields = allowed_fields
        self._backend = backend
        self._model_name = model_name

    def compile_offset(self, query: QuerySpec, *, base_stmt: Any | None = None) -> Any:
        """Compile offset pagination statement."""
        sa_model = self._sa_model
        stmt = base_stmt if base_stmt is not None else select(sa_model)

        if query.filters:
            clause = compile_filter_group(sa_model, query.filters, self._allowed_fields)
            stmt = stmt.where(clause)

        order_clauses = compile_order_by(sa_model, query.sort)
        stmt = stmt.order_by(*(order_clauses or [self._id_column]))

        offset = (query.page - 1) * query.limit
        stmt = stmt.offset(offset).limit(query.limit)

        return stmt

    def compile_cursor(self, query: QuerySpec, *, base_stmt: Any | None = None) -> Any:
        """Compile the cursor pagination statement (N+1 applied).

        Rows are ordered by every sort key with the primary key appended as
        the tie-breaker, so the keyset predicate is total.

        Raises:
            UnsupportedQuery: If ``query.cursor`` is invalid for this backend
                or does not match ``query.sort``.
        """
        sa_model = self._sa_model
        stmt = base_stmt if base_stmt is not None else select(sa_model)

        if query.filters:
            clause = compile_filter_group(sa_model, query.filters, self._allowed_fields)
            stmt = stmt.where(clause)

        if query.cursor:
            cursor = self._decode(query.cursor, query)
            predicate = compile_cursor_predicate(sa_model, cursor, query.sort, self._id_column)
            stmt = stmt.where(predicate)

        order_clauses = compile_order_by(sa_model, query.sort)
        stmt = stmt.order_by(*order_clauses, self._id_column)
        return stmt.limit(query.limit + 1)

    def paginate_cursor(
        self, items: list[Any], query: QuerySpec
    ) -> tuple[list[Any], str | None, bool]:
        """Truncate ``limit + 1`` rows to a page and issue the next token."""
        return extract_next_cursor(
            items, query.sort, self._id_column.key, query.limit, self._backend
        )

    def _decode(self, token: str, query: QuerySpec) -> Cursor:
        cursor = decode_cursor(token, self._backend, self._model_name)
        if len(cursor.keys) != len(query.sort):
            raise UnsupportedQuery(
                self._backend, self._model_name, "cursor token does not match the sort"
            )
        return cursor

    async def execute(
        self,
        session: AsyncSession,
        query: QuerySpec,
        profile_options: list[Any],
    ) -> tuple[list[Any], int | None, str | None, bool]:
        """Execute the query and return raw results for the mixin to convert.

        Args:
            session: Active async SQLAlchemy session.
            query: Structured query specification.
            profile_options: SQLAlchemy loader options for the active profile.

        Returns:
            Tuple of ``(orm_objects, total_count, next_cursor, has_next)``.
            ``total_count`` is ``None`` for cursor mode.
            ``next_cursor`` is ``None`` for offset mode.
        """
        if query.pagination == PaginationMode.CURSOR:
            return await self._execute_cursor(session, query, profile_options)
        return await self._execute_offset(session, query, profile_options)

    # ------------------------------------------------------------------
    # Offset pagination
    # ------------------------------------------------------------------

    async def _execute_offset(
        self,
        session: AsyncSession,
        query: QuerySpec,
        profile_options: list[Any],
    ) -> tuple[list[Any], int, None, bool]:
        stmt = self.compile_offset(query).add_columns(
            func.count().over().label("__loom_total_count")
        )
        if profile_options:
            stmt = stmt.options(*profile_options)

        result = await session.execute(stmt)
        rows = list(result.mappings().all())
        total_count = int(rows[0]["__loom_total_count"] or 0) if rows else 0
        items = [row[self._sa_model] for row in rows]
        offset = (query.page - 1) * query.limit
        has_next = (offset + len(items)) < total_count

        return items, total_count, None, has_next

    # ------------------------------------------------------------------
    # Cursor pagination
    # ------------------------------------------------------------------

    async def _execute_cursor(
        self,
        session: AsyncSession,
        query: QuerySpec,
        profile_options: list[Any],
    ) -> tuple[list[Any], None, str | None, bool]:
        stmt = self.compile_cursor(query)
        if profile_options:
            stmt = stmt.options(*profile_options)

        result = await session.execute(stmt)
        raw_items = list(result.scalars().all())

        page_items, next_cursor, has_next = self.paginate_cursor(raw_items, query)
        return page_items, None, next_cursor, has_next
