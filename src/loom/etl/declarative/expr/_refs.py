"""Table and column references for the ETL declarative DSL."""

from __future__ import annotations

from loom.etl.declarative.expr._predicate import _ColOps


class TableRef:
    """Logical table identifier used by the ETL declarative DSL.

    Args:
        ref: Dotted logical table reference (for example ``"raw.orders"``).
    """

    __slots__ = ("_ref",)

    def __init__(self, ref: str) -> None:
        self._ref = ref

    @property
    def ref(self) -> str:
        """Raw dotted table reference."""
        return self._ref

    @property
    def c(self) -> _ColumnNamespace:
        """Column namespace for bound references."""
        return _ColumnNamespace(self)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TableRef):
            return NotImplemented
        return self._ref == other._ref

    def __hash__(self) -> int:
        return hash(self._ref)

    def __repr__(self) -> str:
        return f"TableRef({self._ref!r})"


class _ColumnNamespace:
    __slots__ = ("_table",)

    def __init__(self, table: TableRef) -> None:
        self._table = table

    def __getattr__(self, name: str) -> ColumnRef:
        if name.startswith("_"):
            raise AttributeError(name)
        return ColumnRef(table=self._table, name=name)


class ColumnRef(_ColOps):
    """Column reference bound to a specific :class:`TableRef`."""

    __slots__ = ("_table", "_name")

    def __init__(self, table: TableRef, name: str) -> None:
        self._table = table
        self._name = name

    @property
    def table(self) -> TableRef:
        """Table this column belongs to."""
        return self._table

    @property
    def name(self) -> str:
        """Column name."""
        return self._name

    def __hash__(self) -> int:
        return hash((self._table, self._name))

    def __repr__(self) -> str:
        return f"{self._table.ref}.c.{self._name}"


class UnboundColumnRef(_ColOps):
    """Column reference not yet bound to a specific table."""

    __slots__ = ("_name",)

    def __init__(self, name: str) -> None:
        self._name = name

    @property
    def name(self) -> str:
        """Column name."""
        return self._name

    def __hash__(self) -> int:
        return hash(self._name)

    def __repr__(self) -> str:
        return f"col({self._name!r})"


def col(name: str) -> UnboundColumnRef:
    """Return an unbound column reference for DSL predicates."""
    return UnboundColumnRef(name)


__all__ = ["TableRef", "ColumnRef", "UnboundColumnRef", "col"]
