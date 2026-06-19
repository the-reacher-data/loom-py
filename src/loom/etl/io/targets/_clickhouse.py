"""ClickHouse target — IntoClickHouse builder, ClickHouseTableSpec, and ClickHouseClientExecutor."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

import polars as pl

try:
    import clickhouse_connect as _clickhouse_connect  # type: ignore[import-untyped]
except ImportError:
    _clickhouse_connect = None

from loom.etl.declarative.target._client import ClientSpec

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
        if _clickhouse_connect is None:
            raise ImportError(
                "ClickHouseTargetWriter requires 'clickhouse-connect'. "
                "Install it with: pip install 'loom-py[clickhouse]'"
            )
        self._client = _clickhouse_connect.get_client(dsn=url)

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
        if isinstance(spec, ClientSpec):
            raise TypeError(
                "ClickHouseTargetWriter.write() does not handle ClientSpec. "
                "ClientStep execution is routed by ETLExecutor before write() is called. "
                "This is an internal error — ClientSpec should never reach write()."
            )
        _ = write_ctx
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


class ClickHouseClientExecutor:
    """Provides a ClickHouse client to :class:`~loom.etl.ClientStep` instances.

    Implements :class:`~loom.etl.runtime.contracts.ClientCommandExecutor`.
    Inject into :class:`~loom.etl.executor.ETLExecutor` (or
    :class:`~loom.etl.ETLRunner`) to enable ``ClientStep`` execution against
    ClickHouse.

    Args:
        url:    ClickHouse DSN, e.g. ``clickhouse://user:pass@host:8123/db``.
                Mutually exclusive with *client*.
        client: Pre-built ``clickhouse_connect`` client.  Use for testing or
                when the client is managed externally.  Mutually exclusive
                with *url*.

    Raises:
        ImportError: If ``clickhouse-connect`` is not installed and *client*
            is not provided.
        ValueError: If neither *url* nor *client* is provided when
            :meth:`command` is called.

    Example::

        from loom.etl import ETLRunner
        from loom.etl.io import ClickHouseClientExecutor

        runner = ETLRunner(
            reader=ch_reader,
            writer=ch_writer,
            client_executor=ClickHouseClientExecutor(url="clickhouse://user:pass@host:8123/db"),
        )
    """

    def __init__(self, url: str | None = None, *, client: Any | None = None) -> None:
        self._url = url
        self._client = client
        self._client_lock = threading.Lock()

    def command(self, fn: Callable[[Any], None]) -> None:
        """Resolve the ClickHouse client and invoke *fn* with it.

        Args:
            fn: Callable produced by the executor that accepts the raw
                ``clickhouse_connect`` client and executes the step logic.

        Raises:
            ValueError: If neither a URL nor a pre-built client was provided.
            ImportError: If ``clickhouse-connect`` is not installed.
        """
        fn(self._client or self._get_client())

    def _get_client(self) -> Any:
        with self._client_lock:
            if self._client is not None:
                return self._client
            if not self._url:
                raise ValueError(
                    "ClickHouseClientExecutor requires either 'url' or 'client' "
                    "at construction time."
                )
            if _clickhouse_connect is None:
                raise ImportError(
                    "ClickHouseClientExecutor requires 'clickhouse-connect'. "
                    "Install it with: pip install 'loom-py[clickhouse]'"
                )
            self._client = _clickhouse_connect.get_client(dsn=self._url)
            return self._client


__all__ = [
    "ClickHouseTableSpec",
    "ClickHouseTargetWriter",
    "IntoClickHouse",
    "ClickHouseClientExecutor",
]
