"""ClickHouse target — IntoClickHouse builder and ClickHouseTableSpec."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Literal

_log = logging.getLogger(__name__)


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

    Uses the native HTTP/2 columnar insert protocol — no SQLAlchemy required.

    Args:
        url: ClickHouse DSN, e.g. ``clickhouse://user:pass@host:8123/default``.

    Raises:
        ImportError: If ``clickhouse-connect`` is not installed.
    """

    def __init__(self, url: str) -> None:
        try:
            import clickhouse_connect as _cc  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError(
                "ClickHouseTargetWriter requires 'clickhouse-connect'. "
                "Install it with: pip install 'loom-py[clickhouse]'"
            ) from exc
        self._client = _cc.get_client(dsn=url)

    def write(
        self,
        frame: Any,
        spec: ClickHouseTableSpec,
        params: Any,
        /,
        *,
        streaming: bool = False,
        write_ctx: Any = None,
    ) -> None:
        """Write *frame* to the ClickHouse table described by *spec*.

        For ``replace`` mode the table is truncated before the insert so the
        operation is idempotent (same semantics as ``dbt full_refresh``).
        The truncation window is typically 3–15 s; plan accordingly.

        Args:
            frame:     Polars ``LazyFrame`` or ``DataFrame``.
            spec:      Compiled ClickHouse target spec.
            params:    ETL step params (unused — reserved for future predicate
                       parametrization).
            streaming: Ignored — ClickHouse inserts are always materialised
                       before the network call.
            write_ctx: Execution context for audit-column injection (unused
                       in ClickHouse targets).
        """
        _ = write_ctx
        import polars as pl

        engine: Literal["streaming", "auto"] = "streaming" if streaming else "auto"
        df: pl.DataFrame = (
            frame.collect(engine=engine) if isinstance(frame, pl.LazyFrame) else frame
        )
        if df.is_empty():
            _log.debug("clickhouse write skipped — empty frame table=%s", spec.table)
            return

        qualified = f"{spec.database}.{spec.table}" if spec.database else spec.table

        if spec.write_mode == "replace":
            _log.debug("clickhouse TRUNCATE TABLE IF EXISTS %s", qualified)
            self._client.command(f"TRUNCATE TABLE IF EXISTS {qualified}")

        column_names = df.columns
        data = [df[col].to_list() for col in column_names]
        _log.debug(
            "clickhouse INSERT rows=%d cols=%d table=%s",
            len(df),
            len(column_names),
            qualified,
        )
        self._client.insert(
            qualified,
            data,
            column_names=column_names,
            column_oriented=True,
        )


__all__ = ["ClickHouseTableSpec", "ClickHouseTargetWriter", "IntoClickHouse"]
