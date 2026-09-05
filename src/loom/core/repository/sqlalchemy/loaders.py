from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Sequence
from typing import Any, Protocol

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.selectable import FromClause

from loom.core.projection.loaders import CountLoader, ExistsLoader, JoinFieldsLoader


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


SqlLoaderFactory = Callable[[Any, FromClause, str], Any]


def _build_sql_count_loader(
    descriptor: CountLoader, table: FromClause, fk_col: str
) -> _SqlCountLoader:
    """Build the SQL count loader for a :class:`CountLoader` descriptor."""
    _ = descriptor
    return _SqlCountLoader(table=table, fk_col=fk_col)


def _build_sql_exists_loader(
    descriptor: ExistsLoader, table: FromClause, fk_col: str
) -> _SqlExistsLoader:
    """Build the SQL existence loader for an :class:`ExistsLoader` descriptor."""
    _ = descriptor
    return _SqlExistsLoader(table=table, fk_col=fk_col)


def _build_sql_join_fields_loader(
    descriptor: JoinFieldsLoader, table: FromClause, fk_col: str
) -> _SqlJoinFieldsLoader:
    """Build the SQL column loader for a :class:`JoinFieldsLoader` descriptor."""
    return _SqlJoinFieldsLoader(table=table, fk_col=fk_col, value_columns=descriptor.value_columns)


SQL_LOADER_FACTORIES: dict[type, SqlLoaderFactory] = {
    CountLoader: _build_sql_count_loader,
    ExistsLoader: _build_sql_exists_loader,
    JoinFieldsLoader: _build_sql_join_fields_loader,
}
"""SQL-path counterpart of every public projection descriptor.

This is the one place the SQL layer has to grow when a descriptor is added;
``tests/unit/core/projection/test_descriptors.py`` fails while an entry is
missing.
"""


def _find_sql_loader_factory(loader_type: type) -> SqlLoaderFactory | None:
    """Look up the factory for *loader_type*, honouring descriptor subclasses."""
    for klass in loader_type.__mro__:
        factory = SQL_LOADER_FACTORIES.get(klass)
        if factory is not None:
            return factory
    return None


def make_sql_loader(loader: Any, rel_step: Any) -> Any:
    """Create an SQL-path loader from a public descriptor and a compiled relation step.

    Args:
        loader: A projection loader descriptor, or any custom loader.
        rel_step: A ``CoreRelationStep`` providing ``target_table`` and ``fk_col``.

    Returns:
        The SQL loader built by the matching factory, or ``loader`` unchanged
        when no factory covers its type.
    """
    factory = _find_sql_loader_factory(type(loader))
    if factory is None:
        return loader
    return factory(loader, rel_step.target_table, rel_step.fk_col)
