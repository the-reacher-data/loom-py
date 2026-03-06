from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import Any, Protocol

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.selectable import FromClause


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


# ---------------------------------------------------------------------------
# Internal SQL-path loaders — synthesized at compile_all() time
# ---------------------------------------------------------------------------


class _SqlCountLoader:
    """SQL loader that counts related rows per parent id."""

    __slots__ = ("_table", "_fk_col")

    def __init__(self, *, table: FromClause, fk_col: str) -> None:
        self._table = table
        self._fk_col = fk_col

    async def load_many(
        self, session: AsyncSession, parent_ids: Sequence[object]
    ) -> dict[object, int]:
        if not parent_ids:
            return {}
        fk_column = self._table.c[self._fk_col]
        stmt = (
            select(fk_column, func.count().label("count"))
            .where(fk_column.in_(list(parent_ids)))
            .group_by(fk_column)
        )
        rows = await session.execute(stmt)
        return {row[0]: int(row[1]) for row in rows.all()}


class _SqlExistsLoader:
    """SQL loader that checks existence of related rows per parent id."""

    __slots__ = ("_table", "_fk_col")

    def __init__(self, *, table: FromClause, fk_col: str) -> None:
        self._table = table
        self._fk_col = fk_col

    async def load_many(
        self, session: AsyncSession, parent_ids: Sequence[object]
    ) -> dict[object, bool]:
        if not parent_ids:
            return {}
        fk_column = self._table.c[self._fk_col]
        stmt = select(fk_column).where(fk_column.in_(list(parent_ids))).distinct()
        rows = await session.execute(stmt)
        return {row[0]: True for row in rows.all()}


class _SqlJoinFieldsLoader:
    """SQL loader that fetches selected columns from a related table, grouped by parent."""

    __slots__ = ("_table", "_fk_col", "_value_columns")

    def __init__(
        self,
        *,
        table: FromClause,
        fk_col: str,
        value_columns: tuple[str, ...],
    ) -> None:
        self._table = table
        self._fk_col = fk_col
        self._value_columns = value_columns

    async def load_many(
        self, session: AsyncSession, parent_ids: Sequence[object]
    ) -> dict[object, list[dict[str, Any]]]:
        if not parent_ids:
            return {}
        fk_column = self._table.c[self._fk_col]
        selected = [fk_column] + [self._table.c[col] for col in self._value_columns]
        stmt = select(*selected).where(fk_column.in_(list(parent_ids)))
        rows = await session.execute(stmt)
        grouped: dict[object, list[dict[str, Any]]] = defaultdict(list)
        for row in rows.all():
            parent_id = row[0]
            payload = {name: row[i + 1] for i, name in enumerate(self._value_columns)}
            grouped[parent_id].append(payload)
        return dict(grouped)


def make_sql_loader(loader: Any, rel_step: Any) -> Any:
    """Create an SQL-path loader from a public descriptor and a compiled relation step.

    Args:
        loader: A ``CountLoader``, ``ExistsLoader``, or ``JoinFieldsLoader`` descriptor.
        rel_step: A ``CoreRelationStep`` providing ``target_table`` and ``fk_col``.

    Returns:
        The corresponding ``_Sql*`` loader instance, or ``loader`` unchanged
        if it is not a recognised descriptor type.
    """
    from loom.core.projection.loaders import CountLoader, ExistsLoader, JoinFieldsLoader

    table: FromClause = rel_step.target_table
    fk_col: str = rel_step.fk_col

    if isinstance(loader, CountLoader):
        return _SqlCountLoader(table=table, fk_col=fk_col)
    if isinstance(loader, ExistsLoader):
        return _SqlExistsLoader(table=table, fk_col=fk_col)
    if isinstance(loader, JoinFieldsLoader):
        return _SqlJoinFieldsLoader(table=table, fk_col=fk_col, value_columns=loader.value_columns)
    return loader
