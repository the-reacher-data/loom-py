"""ClickHouse source — FromClickHouse builder and ClickHouseSourceSpec."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from loom.etl.compiler._errors import ETLCompilationError, ETLErrorCode  # noqa: F401
from loom.etl.declarative.source._specs import SourceKind


@dataclass(frozen=True)
class ClickHouseSourceSpec:
    """Compiled spec for a ClickHouse read source.

    Produced by :meth:`FromClickHouse._to_spec`. Consumed by the executor
    and :class:`ClickHouseSourceReader` — never constructed directly in user code.
    """

    alias: str
    table: str
    predicates: tuple[Any, ...] = field(default_factory=tuple)
    columns: tuple[str, ...] = field(default_factory=tuple)
    distinct: bool = False
    allow_full_scan: bool = False

    @property
    def kind(self) -> SourceKind:
        """Source kind — always :attr:`SourceKind.CLICKHOUSE`."""
        return SourceKind.CLICKHOUSE


class FromClickHouse:
    """Declare a ClickHouse table as an ETL source.

    Predicates are required unless :meth:`unbounded` is called explicitly,
    guarding against accidental full-table scans on large tables.

    Args:
        table: ClickHouse table name.

    Example::

        cdc = FromClickHouse("cdc_events")
            .where(
                (col("source_collection") == params.collection)
                & (col("source_time").date() == params.run_date)
            )
            .select(["document_id"])
            .distinct()
    """

    __slots__ = ("_table", "_predicates", "_columns", "_distinct", "_allow_full_scan")

    def __init__(self, table: str) -> None:
        self._table: str = table
        self._predicates: tuple[Any, ...] = ()
        self._columns: tuple[str, ...] = ()
        self._distinct: bool = False
        self._allow_full_scan: bool = False

    def where(self, *predicates: Any) -> FromClickHouse:
        """Add filter predicates (AND-combined at query time)."""
        return self._clone(_predicates=self._predicates + predicates)

    def select(self, columns: list[str]) -> FromClickHouse:
        """Project to a subset of columns at scan time."""
        return self._clone(_columns=tuple(columns))

    def distinct(self) -> FromClickHouse:
        """Add DISTINCT to the generated query."""
        return self._clone(_distinct=True)

    def unbounded(self) -> FromClickHouse:
        """Opt-in to a full-table scan — required when no predicates are used."""
        return self._clone(_allow_full_scan=True)

    def _to_spec(self, alias: str) -> ClickHouseSourceSpec:
        """Compile to a frozen spec for the executor."""
        if not self._predicates and not self._allow_full_scan:
            raise ETLCompilationError(
                code=ETLErrorCode.MISSING_SOURCE_PARAMS,
                component=f"FromClickHouse({self._table!r})",
                message=(
                    f"FromClickHouse({self._table!r}) has no predicates. "
                    "Use .where() to filter rows or .unbounded() for an explicit full scan."
                ),
            )
        return ClickHouseSourceSpec(
            alias=alias,
            table=self._table,
            predicates=self._predicates,
            columns=self._columns,
            distinct=self._distinct,
            allow_full_scan=self._allow_full_scan,
        )

    def _clone(self, **overrides: Any) -> FromClickHouse:
        new = object.__new__(FromClickHouse)
        for slot in self.__slots__:
            object.__setattr__(new, slot, overrides.get(slot, getattr(self, slot)))
        return new

    def __repr__(self) -> str:
        return f"FromClickHouse({self._table!r})"


class ClickHouseSourceReader:
    """Reads a ClickHouseSourceSpec into a Polars LazyFrame via clickhouse-connect.

    Stub implementation — full SQL generation and execution will be wired in T4.
    """

    def read(self, spec: Any, params_instance: Any, /) -> Any:
        raise NotImplementedError("ClickHouseSourceReader not yet implemented")


__all__ = ["ClickHouseSourceReader", "ClickHouseSourceSpec", "FromClickHouse"]
