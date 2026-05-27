"""ClickHouse target — IntoClickHouse builder and ClickHouseTableSpec."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


@dataclass(frozen=True)
class ClickHouseTableSpec:
    """Compiled spec for a ClickHouse write target.

    Produced by :meth:`IntoClickHouse._to_spec`. Consumed by the executor
    and :class:`ClickHouseTargetWriter` — never constructed directly in user code.
    """

    table: str
    write_mode: Literal["replace", "append"] = "append"
    database: str | None = None

    @property
    def kind(self) -> str:
        """Target kind identifier."""
        return "clickhouse"


class IntoClickHouse:
    """Declare a ClickHouse table as the write target for an ETL step.

    Args:
        table:    Target table name.
        database: Optional database override.  When omitted, the executor
                  uses the database configured in the storage section.

    Example::

        target = IntoClickHouse("motos").replace()
        target = IntoClickHouse("moto_features", database="analytics").append()
    """

    __slots__ = ("_table", "_database", "_write_mode")

    def __init__(self, table: str, *, database: str | None = None) -> None:
        self._table: str = table
        self._database: str | None = database
        self._write_mode: Literal["replace", "append"] = "append"

    def replace(self) -> IntoClickHouse:
        """Set write mode to TRUNCATE + INSERT (idempotent full overwrite)."""
        return self._clone(_write_mode="replace")

    def append(self) -> IntoClickHouse:
        """Set write mode to INSERT without truncate."""
        return self._clone(_write_mode="append")

    def _to_spec(self) -> ClickHouseTableSpec:
        """Compile to a frozen spec for the executor."""
        return ClickHouseTableSpec(
            table=self._table,
            write_mode=self._write_mode,
            database=self._database,
        )

    def _clone(self, **overrides: Any) -> IntoClickHouse:
        new = object.__new__(IntoClickHouse)
        for slot in self.__slots__:
            object.__setattr__(new, slot, overrides.get(slot, getattr(self, slot)))
        return new

    def __repr__(self) -> str:
        return f"IntoClickHouse({self._table!r}, mode={self._write_mode!r})"


class ClickHouseTargetWriter:
    """Writes a Polars DataFrame to ClickHouse via clickhouse-connect.

    Stub implementation — full columnar insert will be wired in T4.
    """

    def write(self, spec: ClickHouseTableSpec, frame: Any) -> None:
        raise NotImplementedError("ClickHouseTargetWriter not yet implemented")


__all__ = ["ClickHouseTableSpec", "ClickHouseTargetWriter", "IntoClickHouse"]
