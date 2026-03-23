"""ETL source declaration types.

Public API:  ``FromTable``, ``FromFile``, ``Sources``, ``SourceSet``.
Internal:    ``SourceSpec``, ``SourceKind`` — used by the compiler only.

Three equivalent authoring forms are supported:

* **Form 1 — inline attributes**::

      class MyStep(ETLStep[P]):
          orders = FromTable("raw.orders").where(...)
          customers = FromTable("raw.customers")

* **Form 2 — grouped** ``Sources``::

      class MyStep(ETLStep[P]):
          sources = Sources(
              orders=FromTable("raw.orders").where(...),
              customers=FromTable("raw.customers"),
          )

* **Form 3 — reusable** ``SourceSet``::

      class OrderSources(SourceSet[P]):
          orders = FromTable("raw.orders").where(...)

      class MyStep(ETLStep[P]):
          sources = OrderSources.extended(
              customers=FromTable("raw.customers"),
          )

All three normalize to ``tuple[SourceSpec, ...]`` during compilation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Generic, TypeVar

from loom.etl._format import Format
from loom.etl._predicate import PredicateNode
from loom.etl._table import TableRef

ParamsT = TypeVar("ParamsT")


# ---------------------------------------------------------------------------
# Internal normalized type
# ---------------------------------------------------------------------------


class SourceKind(StrEnum):
    """Physical kind of an ETL source."""

    TABLE = "table"
    FILE = "file"


@dataclass(frozen=True)
class SourceSpec:
    """Normalized internal representation of one ETL source.

    Produced by :meth:`FromTable._to_spec` and :meth:`FromFile._to_spec`.
    Consumed by the compiler and executor — never exposed in user code.

    Args:
        alias:      Name matching the ``execute()`` parameter.
        kind:       Physical kind (table or file).
        table_ref:  Logical table reference (``SourceKind.TABLE`` only).
        path:       File path template (``SourceKind.FILE`` only).
        format:     I/O format.
        predicates: Compiled predicate nodes from ``.where()``.
    """

    alias: str
    kind: SourceKind
    format: Format
    predicates: tuple[Any, ...] = field(default_factory=tuple)
    table_ref: TableRef | None = None
    path: str | None = None


# ---------------------------------------------------------------------------
# User-facing source builders
# ---------------------------------------------------------------------------


class FromTable:
    """Declare a Delta table as an ETL source.

    Accepts a dotted logical reference or an explicit :class:`~loom.etl.TableRef`.
    Predicates are added via :meth:`where` using the standard operator DSL.

    Args:
        ref: Logical table reference — ``str`` or :class:`~loom.etl.TableRef`.

    Example::

        orders = FromTable("raw.orders").where(
            (col("year")  == params.run_date.year)
            & (col("month") == params.run_date.month),
        )
    """

    __slots__ = ("_ref", "_predicates")

    def __init__(self, ref: str | TableRef) -> None:
        self._ref: TableRef = TableRef(ref) if isinstance(ref, str) else ref
        self._predicates: tuple[PredicateNode, ...] = ()

    @property
    def table_ref(self) -> TableRef:
        """The logical table reference."""
        return self._ref

    @property
    def predicates(self) -> tuple[PredicateNode, ...]:
        """Declared filter predicates."""
        return self._predicates

    def where(self, *predicates: Any) -> FromTable:
        """Return a new ``FromTable`` with the given predicates added.

        Args:
            *predicates: One or more :data:`~loom.etl._predicate.PredicateNode`
                expressions built with the column / param operator DSL.

        Returns:
            New ``FromTable`` instance (original is unchanged).
        """
        new = FromTable(self._ref)
        object.__setattr__(new, "_predicates", predicates)  # bypass __slots__ via object
        return new

    def _to_spec(self, alias: str) -> SourceSpec:
        return SourceSpec(
            alias=alias,
            kind=SourceKind.TABLE,
            format=Format.DELTA,
            predicates=self._predicates,
            table_ref=self._ref,
        )

    def __repr__(self) -> str:
        return f"FromTable({self._ref.ref!r})"


class FromFile:
    """Declare a file-based source (CSV, JSON, XLSX, Parquet).

    The ``path`` supports ``{field_name}`` template placeholders that the
    executor resolves from the concrete params at runtime.

    Args:
        path:   File path or template, e.g. ``"s3://raw/orders_{run_date}.csv"``.
        format: :class:`~loom.etl.Format` of the file.

    Example::

        report = FromFile("s3://raw/report_{run_date}.xlsx", format=Format.XLSX)
    """

    __slots__ = ("_path", "_format")

    def __init__(self, path: str, *, format: Format) -> None:
        self._path = path
        self._format = format

    @property
    def path(self) -> str:
        """File path template."""
        return self._path

    @property
    def format(self) -> Format:
        """I/O format."""
        return self._format

    def _to_spec(self, alias: str) -> SourceSpec:
        return SourceSpec(
            alias=alias,
            kind=SourceKind.FILE,
            format=self._format,
            path=self._path,
        )

    def __repr__(self) -> str:
        return f"FromFile({self._path!r}, format={self._format!r})"


# ---------------------------------------------------------------------------
# Source grouping types
# ---------------------------------------------------------------------------

_SourceEntry = FromTable | FromFile


class Sources:
    """Group multiple source declarations under a single ``sources`` attribute.

    Keyword arguments become the source aliases used in ``execute()``
    parameter names.

    Args:
        **sources: Mapping of alias → source declaration.

    Example::

        sources = Sources(
            orders=FromTable("raw.orders").where(...),
            customers=FromTable("raw.customers"),
        )
    """

    __slots__ = ("_sources",)

    def __init__(self, **sources: _SourceEntry) -> None:
        self._sources: dict[str, _SourceEntry] = dict(sources)

    @property
    def aliases(self) -> tuple[str, ...]:
        """Declared source aliases in insertion order."""
        return tuple(self._sources)

    def _to_specs(self) -> tuple[SourceSpec, ...]:
        return tuple(src._to_spec(alias) for alias, src in self._sources.items())

    def __repr__(self) -> str:
        return f"Sources({', '.join(self._sources)})"


class SourceSet(Generic[ParamsT]):
    """Reusable named group of sources that can be extended per step.

    Declare shared reads once and compose them into individual steps via
    :meth:`extended`.

    Args:
        **sources: Base source declarations.

    Example::

        class OrderSources(SourceSet[DailyOrdersParams]):
            orders = FromTable("raw.orders").where(...)
            customers = FromTable("raw.customers")

        class MyStep(ETLStep[DailyOrdersParams]):
            sources = OrderSources.extended(
                brands=FromTable("erp.brands"),
            )
    """

    # Class-level sources populated by __init_subclass__
    _sources: dict[str, _SourceEntry]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls._sources = {
            name: val
            for name, val in cls.__dict__.items()
            if isinstance(val, (FromTable, FromFile))
        }

    @classmethod
    def extended(cls, **extra: _SourceEntry) -> SourceSet[ParamsT]:
        """Return a new :class:`SourceSet` with additional sources merged in.

        Args:
            **extra: Additional source declarations to add.

        Returns:
            New ``SourceSet`` with the combined sources.

        Raises:
            ValueError: If any key in ``extra`` conflicts with an existing alias.
        """
        conflicts = set(cls._sources) & set(extra)
        if conflicts:
            raise ValueError(f"SourceSet.extended(): conflicting source names: {sorted(conflicts)}")
        merged: dict[str, _SourceEntry] = {**cls._sources, **extra}
        instance = object.__new__(SourceSet)
        instance._sources = merged
        return instance

    def _to_specs(self) -> tuple[SourceSpec, ...]:
        sources = getattr(self, "_sources", {})
        return tuple(src._to_spec(alias) for alias, src in sources.items())

    def __repr__(self) -> str:
        names = ", ".join(getattr(self, "_sources", {}))
        return f"SourceSet({names})"
