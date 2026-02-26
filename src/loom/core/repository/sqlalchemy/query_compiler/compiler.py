"""QuerySpecCompiler — orchestrates filter, ordering, and pagination compilation."""

from __future__ import annotations

from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

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

    Example::

        compiler = QuerySpecCompiler(ProductSA, ProductSA.id, frozenset())
        result = await compiler.execute(session, query, profile_options)
    """

    def __init__(
        self,
        sa_model: type[Any],
        id_column: Any,
        allowed_fields: frozenset[str],
    ) -> None:
        self._sa_model = sa_model
        self._id_column = id_column
        self._allowed_fields = allowed_fields

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
        sa_model = self._sa_model
        stmt = select(sa_model)
        count_stmt = select(func.count()).select_from(sa_model)

        if query.filters:
            clause = compile_filter_group(sa_model, query.filters, self._allowed_fields)
            stmt = stmt.where(clause)
            count_stmt = count_stmt.where(clause)

        order_clauses = compile_order_by(sa_model, query.sort)
        stmt = stmt.order_by(*(order_clauses or [self._id_column]))

        if profile_options:
            stmt = stmt.options(*profile_options)

        offset = (query.page - 1) * query.limit
        stmt = stmt.offset(offset).limit(query.limit)

        items_result = await session.execute(stmt)
        total_result = await session.execute(count_stmt)

        items = list(items_result.scalars().all())
        total_count = int(total_result.scalar() or 0)
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
        sa_model = self._sa_model

        # Determine cursor field from the first sort spec, fallback to id
        cursor_field = query.sort[0].field if query.sort else self._id_column.key

        stmt = select(sa_model)

        if query.filters:
            clause = compile_filter_group(sa_model, query.filters, self._allowed_fields)
            stmt = stmt.where(clause)

        if query.cursor:
            cursor_predicate = compile_cursor_predicate(sa_model, query.cursor)
            stmt = stmt.where(cursor_predicate)

        order_clauses = compile_order_by(sa_model, query.sort)
        stmt = stmt.order_by(*(order_clauses or [self._id_column]))

        if profile_options:
            stmt = stmt.options(*profile_options)

        # N+1 trick
        stmt = stmt.limit(query.limit + 1)

        result = await session.execute(stmt)
        raw_items = list(result.scalars().all())

        page_items, next_cursor, has_next = extract_next_cursor(
            raw_items, cursor_field, query.limit
        )

        return page_items, None, next_cursor, has_next
