"""Table and column reference types for ETL source declarations.

:class:`TableRef` is the internal logical identifier for any table.
:func:`col` produces an :class:`UnboundColumnRef` that the compiler
resolves to the enclosing source's table.  :class:`ColumnRef` is a
bound reference produced via ``TableRef(...).c.column_name``.

Both reference types support the full predicate operator set so that
where-clause expressions read naturally::

    col("year") == params.run_date.year          # UnboundColumnRef
    orders_ref.c.year == params.run_date.year    # ColumnRef (bound)
"""

from __future__ import annotations

from loom.etl.sql._predicate import _ColOps


class TableRef:
    """Logical table identifier used throughout the ETL compile pipeline.

    Accepts a dotted string (``"db.table"`` or ``"schema.table"``) or
    a plain table name.  Resolution to a physical path or metastore
    entry is deferred to the developer-provided ``TableDiscovery``
    infrastructure.

    Args:
        ref: Dotted logical table reference, e.g. ``"raw.orders"``.

    Example::

        orders = TableRef("raw.orders")
        orders.c.year   # → ColumnRef(table=orders, name="year")
    """

    __slots__ = ("_ref",)

    def __init__(self, ref: str) -> None:
        self._ref = ref

    @property
    def ref(self) -> str:
        """The raw dotted table reference string."""
        return self._ref

    @property
    def c(self) -> _ColumnNamespace:
        """Column namespace for building bound :class:`ColumnRef` instances."""
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
    """Internal namespace returned by ``TableRef.c``.

    Attribute access produces a :class:`ColumnRef` bound to the parent table.
    """

    __slots__ = ("_table",)

    def __init__(self, table: TableRef) -> None:
        self._table = table

    def __getattr__(self, name: str) -> ColumnRef:
        if name.startswith("_"):
            raise AttributeError(name)
        return ColumnRef(table=self._table, name=name)


class ColumnRef(_ColOps):
    """A column reference bound to a specific :class:`TableRef`.

    Produced via ``TableRef(...).c.column_name``.  Supports the full
    predicate operator set for use in ``.where()`` clauses.

    Args:
        table: The table this column belongs to.
        name:  Column name.

    Example::

        orders = TableRef("raw.orders")
        orders.c.year == params.run_date.year   # → EqPred
    """

    __slots__ = ("_table", "_name")

    def __init__(self, table: TableRef, name: str) -> None:
        self._table = table
        self._name = name

    @property
    def table(self) -> TableRef:
        """The table this column is bound to."""
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
    """A column reference not yet bound to a specific table.

    Produced by :func:`col`.  The ETL compiler resolves it against the
    enclosing source's :class:`TableRef` during plan construction.

    Args:
        name: Column name.

    Example::

        col("year") == params.run_date.year   # unbound; resolved by compiler
    """

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
    """Return an unbound column reference for use in ``.where()`` clauses.

    The compiler resolves this against the enclosing source's table.
    Use :attr:`TableRef.c` when explicit column ownership improves
    readability or when the same table is referenced multiple times.

    Args:
        name: Column name.

    Returns:
        Unbound column reference.

    Example::

        FromTable("raw.orders").where(
            col("year") == params.run_date.year,
        )
    """
    return UnboundColumnRef(name)
