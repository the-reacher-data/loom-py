from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Sequence
from typing import Any, Protocol

from sqlalchemy import Select, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.selectable import FromClause

from loom.core.backend.sqlalchemy import get_compiled

TableRef = FromClause | type | Callable[[], FromClause | type]


def _coalesce_table_ref(*, table: TableRef | None, model: type | None) -> TableRef:
    if table is not None:
        return table
    if model is not None:
        return model
    raise ValueError("A loader requires either 'table' or 'model'.")


def _resolve_table_ref(table_ref: TableRef) -> FromClause:
    if isinstance(table_ref, type):
        resolved = table_ref
    elif callable(table_ref):
        resolved = table_ref()
    else:
        resolved = table_ref
    if isinstance(resolved, FromClause):
        return resolved

    direct_table = getattr(resolved, "__table__", None)
    if isinstance(direct_table, FromClause):
        return direct_table

    compiled = get_compiled(resolved)
    compiled_table = getattr(compiled, "__table__", None) if compiled is not None else None
    if isinstance(compiled_table, FromClause):
        return compiled_table

    raise TypeError(
        "Invalid table reference. Use a SQLAlchemy table/from clause "
        "or a compiled/uncompiled model class.",
    )


class ProjectionLoader(Protocol):
    """Protocol for batch-loading derived field values for a set of parent entities."""

    async def load_many(
        self, session: AsyncSession, parent_ids: Sequence[object]
    ) -> dict[object, Any]:
        """Load derived values for the given parent ids.

        Args:
            session: Active SQLAlchemy async session.
            parent_ids: Primary keys of the parent entities.

        Returns:
            Mapping from parent id to the computed value.
        """
        ...


class CountLoader:
    """Projection loader that counts related rows per parent entity."""

    def __init__(
        self,
        *,
        table: TableRef | None = None,
        model: type | None = None,
        foreign_key: str,
        where_clauses: Sequence[ColumnElement[bool]] = (),
    ) -> None:
        """Initialise the count loader.

        Args:
            table: Target table or callable returning it (for deferred resolution).
            model: Model class associated with the table (preferred API for framework users).
            foreign_key: Column name in ``table`` referencing the parent id.
            where_clauses: Additional SQL filter conditions.
        """
        self.table = _coalesce_table_ref(table=table, model=model)
        self.foreign_key = foreign_key
        self.where_clauses = tuple(where_clauses)

    def _table(self) -> FromClause:
        return _resolve_table_ref(self.table)

    async def load_many(
        self, session: AsyncSession, parent_ids: Sequence[object]
    ) -> dict[object, int]:
        """Count related rows grouped by parent id.

        Args:
            session: Active SQLAlchemy async session.
            parent_ids: Primary keys of the parent entities.

        Returns:
            Mapping from parent id to the count of related rows.
        """
        if not parent_ids:
            return {}
        table = self._table()
        fk_column = table.c[self.foreign_key]
        stmt = (
            select(fk_column, func.count().label("count"))
            .where(fk_column.in_(list(parent_ids)))
            .group_by(fk_column)
        )
        for clause in self.where_clauses:
            stmt = stmt.where(clause)
        rows = await session.execute(stmt)
        return {row[0]: int(row[1]) for row in rows.all()}


class ExistsLoader:
    """Projection loader that checks for the existence of related rows per parent entity."""

    def __init__(
        self,
        *,
        table: TableRef | None = None,
        model: type | None = None,
        foreign_key: str,
        where_clauses: Sequence[ColumnElement[bool]] = (),
    ) -> None:
        """Initialise the exists loader.

        Args:
            table: Target table or callable returning it (for deferred resolution).
            model: Model class associated with the table (preferred API for framework users).
            foreign_key: Column name in ``table`` referencing the parent id.
            where_clauses: Additional SQL filter conditions.
        """
        self.table = _coalesce_table_ref(table=table, model=model)
        self.foreign_key = foreign_key
        self.where_clauses = tuple(where_clauses)

    def _table(self) -> FromClause:
        return _resolve_table_ref(self.table)

    async def load_many(
        self, session: AsyncSession, parent_ids: Sequence[object]
    ) -> dict[object, bool]:
        """Check existence of related rows for each parent id.

        Args:
            session: Active SQLAlchemy async session.
            parent_ids: Primary keys of the parent entities.

        Returns:
            Mapping from parent id to ``True`` for those with at least one related row.
        """
        if not parent_ids:
            return {}
        table = self._table()
        fk_column = table.c[self.foreign_key]
        stmt = select(fk_column).where(fk_column.in_(list(parent_ids))).distinct()
        for clause in self.where_clauses:
            stmt = stmt.where(clause)
        rows = await session.execute(stmt)
        return {row[0]: True for row in rows.all()}


class JoinFieldsLoader:
    """Projection loader that fetches selected columns from a related table, grouped by parent."""

    def __init__(
        self,
        *,
        table: TableRef | None = None,
        model: type | None = None,
        foreign_key: str,
        value_columns: Sequence[str],
        where_clauses: Sequence[ColumnElement[bool]] = (),
        order_by: Sequence[Any] = (),
    ) -> None:
        """Initialise the join-fields loader.

        Args:
            table: Target table or callable returning it (for deferred resolution).
            model: Model class associated with the table (preferred API for framework users).
            foreign_key: Column name in ``table`` referencing the parent id.
            value_columns: Column names to select from the related table.
            where_clauses: Additional SQL filter conditions.
            order_by: SQLAlchemy ordering clauses applied to the result rows.
        """
        self.table = _coalesce_table_ref(table=table, model=model)
        self.foreign_key = foreign_key
        self.value_columns = tuple(value_columns)
        self.where_clauses = tuple(where_clauses)
        self.order_by = tuple(order_by)

    def _table(self) -> FromClause:
        return _resolve_table_ref(self.table)

    async def load_many(
        self, session: AsyncSession, parent_ids: Sequence[object]
    ) -> dict[object, list[dict[str, Any]]]:
        """Fetch related rows and group them by parent id.

        Args:
            session: Active SQLAlchemy async session.
            parent_ids: Primary keys of the parent entities.

        Returns:
            Mapping from parent id to a list of dicts with the selected column values.
        """
        if not parent_ids:
            return {}

        table = self._table()
        fk_column = table.c[self.foreign_key]
        selected_columns = [fk_column]
        selected_columns.extend(table.c[column_name] for column_name in self.value_columns)

        stmt: Select[Any] = select(*selected_columns).where(fk_column.in_(list(parent_ids)))
        for clause in self.where_clauses:
            stmt = stmt.where(clause)
        if self.order_by:
            stmt = stmt.order_by(*self.order_by)

        rows = await session.execute(stmt)
        grouped: dict[object, list[dict[str, Any]]] = defaultdict(list)
        for row in rows.all():
            parent_id = row[0]
            payload: dict[str, Any] = {}
            for index, name in enumerate(self.value_columns, start=1):
                payload[name] = row[index]
            grouped[parent_id].append(payload)
        return dict(grouped)
