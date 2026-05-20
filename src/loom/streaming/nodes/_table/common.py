"""IntoTable streaming sink node — writes typed events to Delta or SQLAlchemy tables.

IntoTable is a frozen dataclass that implements IntoSink structurally.
The compiler detects it via isinstance(node, IntoSink) without any
registration or inheritance.

Supported backends
------------------
- ``Backend.SQLALCHEMY``: bulk-inserts rows using SQLAlchemy async sessions.
  Requires an async bridge and a shared SQLAlchemy session manager from the
  Bytewax runtime.
  Install: ``pip install sqlalchemy``.
- ``Backend.DELTA``: stages rows as parquet parts in temporary files and
  appends them to Delta via delta-rs.
  Requires a ``uri`` key in the resolved config section.
  Install: ``pip install deltalake polars``.

Config example (streaming.sinks.<name>)::

    database:
      warehouse:
        url: postgresql+asyncpg://user:pass@host/db
        pool_pre_ping: true
        pool_size: 10

    streaming:
      sinks:
        orders_sink:
          database: warehouse
          table: orders
          chunk_size: 500

        events_sink:
          uri: s3://data-lake/events
          mode: append            # delta write mode (default: append)

SQLAlchemy example::

    from loom.core.config import ConfigContext
    from loom.core.repository.sqlalchemy.session_manager import SessionManager
    from loom.streaming.nodes._table import SqlAlchemyDatabaseConfig, SqlAlchemySinkConfig

    ctx = ConfigContext.from_yaml("config.yaml")
    sink_cfg = SqlAlchemySinkConfig.from_config(
        ctx.section("streaming.sinks.orders_sink", dict),
        default_table="orders",
    )
    db_cfg = SqlAlchemyDatabaseConfig.from_config(
        ctx.section(f"database.{sink_cfg.database}", dict),
    )
    session_manager = SessionManager.from_config(db_cfg.to_session_manager_config())

Delta example::

    from loom.core.config import ConfigContext
    from loom.streaming.nodes._table import DeltaSinkConfig

    ctx = ConfigContext.from_yaml("config.yaml")
    sink_cfg = DeltaSinkConfig.from_config(
        ctx.section("streaming.sinks.events_sink", dict),
        default_table="events",
    )
"""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal
from enum import StrEnum
from tempfile import SpooledTemporaryFile
from time import monotonic
from types import UnionType
from typing import (
    Any,
    ClassVar,
    Generic,
    Literal,
    Protocol,
    Self,
    TypeVar,
    Union,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

import msgspec
import msgspec.structs

from loom.core.async_bridge import AsyncBridge
from loom.streaming.core._message import StreamPayload
from loom.streaming.nodes._sink import SinkPartition

# ── optional SQLAlchemy dependency ────────────────────────────────────────────
# Imported once at module level. _SA_AVAILABLE stays False when sqlalchemy is
# not installed; every class and helper below can reference these names safely
# because from __future__ import annotations makes all annotations lazy strings.
try:
    import sqlalchemy as _sa
    from sqlalchemy import insert as _sa_insert

    Boolean = _sa.Boolean
    Column = _sa.Column
    DateTime = _sa.DateTime
    Integer = _sa.Integer
    MetaData = _sa.MetaData
    Numeric = _sa.Numeric
    SAJSON = _sa.JSON
    SAFloat = _sa.Float
    String = _sa.String
    Table = _sa.Table
    Text = _sa.Text

    # Defined inside the try block so all SA names are provably bound here.
    _SA_TYPE_MAP: dict[type, Any] = {
        int: Integer(),
        float: SAFloat(),
        bool: Boolean(),
        str: String(),
        datetime: DateTime(timezone=True),
        date: String(10),
        time: String(8),
        Decimal: Numeric(),
        bytes: Text(),
    }
    _SA_AVAILABLE = True
except ImportError:
    _sa_insert = cast(Any, None)
    _SA_TYPE_MAP = {}
    _SA_AVAILABLE = False

# ── optional Delta dependency ─────────────────────────────────────────────────
# Fallback assignments in the except branch ensure both names are always bound,
# removing pyright's "possibly unbound" diagnostic on write_batch call sites.
try:
    import polars as pl
    from deltalake.writer import write_deltalake

    _DELTA_AVAILABLE = True
except ImportError:
    pl = cast(Any, None)
    write_deltalake = cast(Any, None)
    _DELTA_AVAILABLE = False

EventT = TypeVar("EventT", bound=StreamPayload)


class _SessionManagerLike(Protocol):
    """Minimal async session manager contract required by IntoTable."""

    def session(self) -> AbstractAsyncContextManager[Any]:
        """Return an async context manager yielding one SQLAlchemy session."""
        ...


_DELTA_MODES: frozenset[str] = frozenset({"error", "append", "ignore"})
_DEFAULT_BUFFER_MAX_RECORDS = 50_000
_DEFAULT_BUFFER_MAX_BYTES = 128 * 1024 * 1024
_DEFAULT_BUFFER_MAX_AGE_S = 60.0
_DEFAULT_FLUSH_ON_SHUTDOWN = True


class Backend(StrEnum):
    """Storage backend used by :class:`IntoTable` to write epoch batches.

    Args:
        SQLALCHEMY: Bulk-insert rows via SQLAlchemy Core. Transport-agnostic —
            works with any SA-supported database.
        DELTA:      Stage parquet parts and append them to a Delta Lake table.
            Requires ``deltalake`` and ``polars`` to be installed.
    """

    SQLALCHEMY = "sqlalchemy"
    DELTA = "delta"


@dataclass(frozen=True, slots=True)
class _TableBufferPolicy:
    """Common buffering policy shared by all IntoTable backends."""

    max_records: int = _DEFAULT_BUFFER_MAX_RECORDS
    max_bytes: int = _DEFAULT_BUFFER_MAX_BYTES
    max_age_s: float = _DEFAULT_BUFFER_MAX_AGE_S
    flush_on_shutdown: bool = _DEFAULT_FLUSH_ON_SHUTDOWN

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> _TableBufferPolicy:
        """Build a buffering policy from a resolved sink config."""
        return cls(
            max_records=_optional_int(
                config.get("buffer_max_records"),
                _DEFAULT_BUFFER_MAX_RECORDS,
            ),
            max_bytes=_optional_int(config.get("buffer_max_bytes"), _DEFAULT_BUFFER_MAX_BYTES),
            max_age_s=float(config.get("buffer_max_age_s", _DEFAULT_BUFFER_MAX_AGE_S)),
            flush_on_shutdown=bool(config.get("flush_on_shutdown", _DEFAULT_FLUSH_ON_SHUTDOWN)),
        )


@dataclass(frozen=True)
class IntoTable(Generic[EventT]):
    """Streaming terminal node that writes typed events to a storage table.

    Implements :class:`~loom.streaming.nodes.IntoSink` structurally — no
    base class or registration required.  The compiler detects it via
    structural ``isinstance(node, IntoSink)`` and resolves its config from
    ``streaming.sinks.<name>``.

    Args:
        payload: Concrete event type written by this sink.
        table:   Target table name.  May be overridden by a ``table`` key in
                 the resolved config section.
        backend: Storage backend.  Defaults to :attr:`Backend.SQLALCHEMY`.
        name:    Config section key (``streaming.sinks.<name>``).  When
                 empty, the node is self-configured from its DSL fields and
                 receives an empty config dict.
        router_branch_safe: Always ``True`` — IntoTable may appear inside
                 Router branches.

    Example::

        Process(
            Decompose(StoreEventExpander),
            Router({
                StoreRow:    Process(
                    IntoTable(payload=StoreRow, table="store", name="store_sink")
                ),
                LanguageRow: Process(
                    IntoTable(payload=LanguageRow, table="language", name="lang_sink")
                ),
            }),
        )
    """

    payload: type[EventT]
    table: str = ""
    backend: Backend = Backend.SQLALCHEMY
    name: str = ""
    router_branch_safe: ClassVar[bool] = True

    def build_partition(
        self,
        config: Mapping[str, Any],
        worker_index: int,
        worker_count: int,
        bridge: AsyncBridge | None = None,
        session_manager: _SessionManagerLike | None = None,
    ) -> SinkPartition[EventT]:
        """Build the per-worker partition for this sink.

        Args:
            config:       Resolved ``streaming.sinks.<name>`` config section.
            worker_index: Zero-based index of the calling worker (unused).
            worker_count: Total number of workers (unused).
            bridge:       Optional adapter-owned async bridge for SQLAlchemy sinks.
            session_manager: Optional adapter-owned SQLAlchemy session manager.

        Returns:
            A :class:`~loom.streaming.nodes.SinkPartition` ready to receive
            ``write_batch`` and ``close`` calls.

        Raises:
            ValueError: If the resolved backend is not supported.
        """
        del worker_index, worker_count
        if self.backend is Backend.SQLALCHEMY:
            sql_settings = SqlAlchemySinkConfig.from_config(config, default_table=self.table)
            if bridge is None:
                raise RuntimeError(
                    "IntoTable backend=sqlalchemy requires an AsyncBridge from the Bytewax runtime."
                )
            if session_manager is None:
                raise RuntimeError(
                    "IntoTable backend=sqlalchemy requires a shared "
                    "SessionManager from the Bytewax runtime."
                )
            return cast(
                SinkPartition[EventT],
                _SQLAlchemyTablePartition(
                    sql_settings,
                    self.payload,
                    bridge,
                    session_manager,
                ),
            )
        if self.backend is Backend.DELTA:
            delta_settings = DeltaSinkConfig.from_config(config, default_table=self.table)
            return cast(
                SinkPartition[EventT],
                _DeltaTablePartition(delta_settings),
            )
        raise ValueError(f"Unsupported backend: {self.backend!r}")


# ── SQLAlchemy helpers ────────────────────────────────────────────────────────
# These functions reference module-level SA names (Column, Integer, …).
# They are only called after _SA_AVAILABLE is confirmed in __init__, so
# NameError cannot occur — no import statements appear inside them.


def _optional_int(value: Any, default: int) -> int:
    if value is None:
        return default
    return int(value)


@dataclass(frozen=True, slots=True)
class SqlAlchemyDatabaseConfig:
    """Resolved SQLAlchemy connection settings for a shared database entry.

    Args:
        url: SQLAlchemy async URL.
        echo: Enable SQL echo logging.
        pool_pre_ping: Check pooled connections before checkout.
        pool_size: Base connection pool size.
        max_overflow: Additional overflow connections.
        pool_timeout: Seconds to wait for a pooled connection.
        pool_recycle: Seconds before recycling pooled connections.
        connect_args: Extra arguments passed to the DBAPI connect call.
    """

    url: str
    echo: bool = False
    pool_pre_ping: bool = True
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: int = 30
    pool_recycle: int = 1800
    connect_args: dict[str, object] = field(default_factory=dict)

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> Self:
        """Build a SQLAlchemy connection config from a resolved YAML section."""
        url = str(config.get("url") or "").strip()
        if not url:
            raise ValueError("SQLAlchemy database config requires a non-empty 'url'.")
        return cls(
            url=url,
            echo=bool(config.get("echo", False)),
            pool_pre_ping=bool(config.get("pool_pre_ping", True)),
            pool_size=_optional_int(config.get("pool_size"), 10),
            max_overflow=_optional_int(config.get("max_overflow"), 20),
            pool_timeout=_optional_int(config.get("pool_timeout"), 30),
            pool_recycle=_optional_int(config.get("pool_recycle"), 1800),
            connect_args=dict(config.get("connect_args") or {}),
        )

    def to_session_manager_config(self) -> dict[str, object]:
        """Convert the config to ``SessionManager.from_config`` keyword mapping."""
        return {
            "url": self.url,
            "echo": self.echo,
            "pool_pre_ping": self.pool_pre_ping,
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "pool_timeout": self.pool_timeout,
            "pool_recycle": self.pool_recycle,
            "connect_args": dict(self.connect_args),
        }


@dataclass(frozen=True, slots=True)
class SqlAlchemySinkConfig:
    """Resolved ``IntoTable`` settings for the SQLAlchemy backend.

    Args:
        database: Shared ``database.<name>`` reference. Empty when using
            inline ``url`` resolution.
        url: Inline SQLAlchemy URL fallback.
        table: Final target table name.
        chunk_size: Number of rows per bulk ``executemany`` call.
        buffer_policy: Common flush policy shared with other backends.
    """

    database: str
    url: str
    table: str
    chunk_size: int
    buffer_policy: _TableBufferPolicy

    @classmethod
    def from_config(cls, config: Mapping[str, Any], *, default_table: str) -> Self:
        """Build SQLAlchemy sink settings from a resolved sink config."""
        table = str(config.get("table") or default_table).strip()
        database = str(config.get("database") or "").strip()
        url = str(config.get("url") or "").strip()
        if not table:
            raise ValueError("SQLAlchemy sink config requires a non-empty table name.")
        return cls(
            database=database,
            url=url,
            table=table,
            chunk_size=_optional_int(config.get("chunk_size"), 500),
            buffer_policy=_TableBufferPolicy.from_config(config),
        )


@dataclass(frozen=True, slots=True)
class DeltaSinkConfig:
    """Resolved ``IntoTable`` settings for the Delta backend.

    Args:
        uri: Delta table URI.
        table: Final target table name.
        mode: delta-rs write mode.
        storage_options: Object-store credentials/options.
        spool_max_bytes: Maximum in-memory bytes before spooling to disk.
        part_max_records: Maximum records per staged parquet part.
        buffer_policy: Common flush policy shared with other backends.
    """

    uri: str
    table: str
    mode: Literal["error", "append", "ignore"]
    storage_options: dict[str, str]
    spool_max_bytes: int
    part_max_records: int
    buffer_policy: _TableBufferPolicy

    @classmethod
    def from_config(cls, config: Mapping[str, Any], *, default_table: str) -> Self:
        """Build Delta sink settings from a resolved sink config."""
        table = str(config.get("table") or default_table).strip()
        uri = str(config.get("uri") or "").strip()
        if not uri:
            raise ValueError("Delta sink config requires a non-empty 'uri'.")
        return cls(
            uri=uri,
            table=table,
            mode=_validate_delta_mode(config.get("mode", "append")),
            storage_options={k: str(v) for k, v in (config.get("storage_options") or {}).items()},
            spool_max_bytes=_optional_int(config.get("spool_max_bytes"), 32 * 1024 * 1024),
            part_max_records=_optional_int(config.get("part_max_records"), 10_000),
            buffer_policy=_TableBufferPolicy.from_config(config),
        )


def _sa_type_for(annotation: Any) -> Any:
    """Map a Python type annotation to a SQLAlchemy column type instance.

    Unwraps ``Optional[T]`` before mapping.  Collections and unmapped types
    fall back to ``JSON()``.  References module-level ``_SA_TYPE_MAP`` —
    no import statements inside.

    Args:
        annotation: A Python type or ``Optional`` / union variant.

    Returns:
        A SQLAlchemy column type instance.
    """
    origin = get_origin(annotation)
    if origin in (Union, UnionType):
        non_none = [a for a in get_args(annotation) if a is not type(None)]
        return _sa_type_for(non_none[0]) if len(non_none) == 1 else SAJSON()
    if origin in (list, tuple, set, dict, frozenset):
        return SAJSON()
    if isinstance(annotation, type):
        return _SA_TYPE_MAP.get(annotation, SAJSON())
    return SAJSON()


def _sa_table_from_struct(table_name: str, payload_type: type[Any]) -> Table:
    """Build a SQLAlchemy Core ``Table`` from a msgspec Struct type.

    Resolves annotations via :func:`~typing.get_type_hints` and maps each
    Python type to the corresponding SA column type via :func:`_sa_type_for`.
    References module-level ``Column``, ``MetaData``, ``Table`` — no imports
    inside.

    Args:
        table_name:   Target table name used in the SQL DDL.
        payload_type: A :class:`~loom.core.model.struct.LoomStruct` or
                      :class:`~loom.core.model.struct.LoomFrozenStruct`
                      subclass whose fields define the column layout.

    Returns:
        A :class:`~sqlalchemy.Table` bound to a fresh :class:`~sqlalchemy.MetaData`.
    """
    hints = get_type_hints(payload_type)
    columns = [
        Column(field.name, _sa_type_for(hints.get(field.name, object)))
        for field in msgspec.structs.fields(payload_type)
    ]
    return Table(table_name, MetaData(), *columns)


def _row_size_bytes(row: Mapping[str, Any]) -> int:
    """Estimate the serialized size of one row for buffer accounting."""
    return len(msgspec.json.encode(dict(row)))


class _SQLAlchemyTablePartition:
    """Bulk-insert epoch batches into a relational table via SQLAlchemy async sessions.

    The partition reuses a shared SessionManager owned by the Bytewax adapter
    and uses the Bytewax async bridge to execute batch inserts without exposing
    event-loop management to callers.
    Column types are inferred from the msgspec Struct field annotations — no
    ORM mapping or reflection required.

    Args:
        settings:        Resolved SQLAlchemy sink config.
        payload_type:    Struct type whose fields map to table columns.
        bridge:          Bytewax async bridge used to run async writes.
        session_manager: Shared SQLAlchemy session manager for this DB config.

    Raises:
        ImportError: If ``sqlalchemy`` is not installed.
    """

    def __init__(
        self,
        settings: SqlAlchemySinkConfig,
        payload_type: type[Any],
        bridge: AsyncBridge,
        session_manager: _SessionManagerLike,
    ) -> None:
        if not _SA_AVAILABLE:
            raise ImportError(
                "SQLAlchemy backend requires 'sqlalchemy'. Install it with: pip install sqlalchemy"
            )
        self._bridge = bridge
        self._session_manager = session_manager
        self._table = _sa_table_from_struct(settings.table, payload_type)
        self._chunk_size = settings.chunk_size
        self._buffer_policy = settings.buffer_policy
        self._buffer: deque[tuple[dict[str, Any], int]] = deque()
        self._buffer_records = 0
        self._buffer_bytes = 0
        self._last_append_at = monotonic()
        self._closed = False

    def write_batch(self, items: Sequence[Any]) -> None:
        """Bulk-insert one epoch batch.

        Args:
            items: Struct instances to insert.  Empty batches are skipped.
        """
        if not items:
            return
        for item in items:
            row = _to_row_dict(item)
            row_bytes = _row_size_bytes(row)
            self._buffer.append((row, row_bytes))
            self._buffer_records += 1
            self._buffer_bytes += row_bytes
            self._last_append_at = monotonic()
            if self._should_flush():
                self._bridge.run(self._flush_async())

    def close(self) -> None:
        """Release partition resources.

        The shared session manager is owned by the adapter runtime and disposed
        centrally when the Bytewax worker shuts down.
        """
        if self._closed:
            return
        self._closed = True
        if self._buffer and self._buffer_policy.flush_on_shutdown:
            self._bridge.run(self._flush_async())

    def _should_flush(self) -> bool:
        if self._closed or not self._buffer:
            return False
        if self._buffer_records >= self._buffer_policy.max_records:
            return True
        if self._buffer_bytes >= self._buffer_policy.max_bytes:
            return True
        return monotonic() - self._last_append_at >= self._buffer_policy.max_age_s

    async def _flush_async(self) -> None:
        statement = _sa_insert(self._table)
        async with self._session_manager.session() as session:
            try:
                while self._buffer:
                    rows: list[dict[str, Any]] = []
                    while self._buffer and len(rows) < self._chunk_size:
                        row, row_bytes = self._buffer.popleft()
                        rows.append(row)
                        self._buffer_records -= 1
                        self._buffer_bytes -= row_bytes
                    if rows:
                        await session.execute(statement, rows)
                await session.commit()
            except Exception:
                await session.rollback()
                raise


def _to_row_dict(item: Any) -> dict[str, Any]:
    """Serialise one LoomStruct instance to a plain dict."""
    return msgspec.structs.asdict(cast(msgspec.Struct, item))


# ── Delta helpers ─────────────────────────────────────────────────────────────


def _validate_delta_mode(value: object) -> Literal["error", "append", "ignore"]:
    mode = str(value)
    if mode not in _DELTA_MODES:
        raise ValueError(
            f"Invalid Delta write mode {mode!r}. Must be one of: {sorted(_DELTA_MODES)}"
        )
    return mode  # type: ignore[return-value]


class _DeltaTablePartition:
    """Stage rows as parquet parts and append them to Delta.

    The partition keeps a per-worker ``SpooledTemporaryFile`` per staged
    event.  Each incoming row is serialized immediately into its own parquet
    part, which stays in RAM until ``spool_max_bytes`` and then spills
    transparently to disk.  Flushes append the staged parquet parts to the
    configured Delta table and then close their spools.

    Args:
        settings: Resolved Delta sink config.

    Raises:
        ImportError: If ``deltalake`` or ``polars`` are not installed.
    """

    def __init__(
        self,
        settings: DeltaSinkConfig,
    ) -> None:
        if not _DELTA_AVAILABLE:
            raise ImportError(
                "Delta backend requires 'deltalake' and 'polars'. "
                "Install them with: pip install deltalake polars"
            )
        self._uri = settings.uri
        self._mode = settings.mode
        self._storage_options = dict(settings.storage_options)
        self._buffer_policy = settings.buffer_policy
        self._spool_max_bytes = settings.spool_max_bytes
        self._part_max_records = settings.part_max_records
        self._staged_parts: list[SpooledTemporaryFile[bytes]] = []
        self._staged_records = 0
        self._staged_bytes = 0
        self._last_flush_at = monotonic()
        self._last_append_at = self._last_flush_at
        self._closed = False

    def write_batch(self, items: Sequence[Any]) -> None:
        """Convert structs to parquet parts and stage them for Delta flush.

        Args:
            items: Struct instances to write.  Empty batches are skipped.
        """
        if not items:
            return
        for item in items:
            self._stage_item(item)
            if self._should_flush():
                self._flush()

    def close(self) -> None:
        """Flush any staged parquet parts and release their backing spools."""
        if self._closed:
            return
        self._closed = True
        try:
            if self._staged_parts and self._buffer_policy.flush_on_shutdown:
                self._flush()
        finally:
            for part in self._staged_parts:
                part.close()
            self._staged_parts = []

    def _stage_item(self, item: Any) -> None:
        row = msgspec.structs.asdict(cast(msgspec.Struct, item))
        if not row:
            return
        frame = pl.DataFrame([row])
        part: SpooledTemporaryFile[bytes] = SpooledTemporaryFile(  # noqa: SIM115
            max_size=self._spool_max_bytes
        )
        frame.write_parquet(part, row_group_size=self._part_max_records)
        part_size = part.tell()
        part.seek(0)
        self._staged_parts.append(part)
        self._staged_records += 1
        self._staged_bytes += part_size
        self._last_append_at = monotonic()

    def _should_flush(self) -> bool:
        if self._closed or not self._staged_parts:
            return False
        if self._staged_records >= self._buffer_policy.max_records:
            return True
        if self._staged_bytes >= self._buffer_policy.max_bytes:
            return True
        return monotonic() - self._last_append_at >= self._buffer_policy.max_age_s

    def _flush(self) -> None:
        parts = self._staged_parts
        if not parts:
            return
        self._staged_parts = []
        self._staged_records = 0
        self._staged_bytes = 0
        try:
            frames: list[Any] = []
            for part in parts:
                part.seek(0)
                frames.append(pl.read_parquet(part))
            frame = frames[0] if len(frames) == 1 else pl.concat(frames, how="vertical_relaxed")
            write_deltalake_any = cast(Any, write_deltalake)
            write_deltalake_any(
                self._uri,
                frame,
                mode=self._mode,
                storage_options=self._storage_options or None,
            )
        finally:
            for part in parts:
                part.close()
            self._last_flush_at = monotonic()


__all__ = [
    "Backend",
    "DeltaSinkConfig",
    "IntoTable",
    "SqlAlchemyDatabaseConfig",
    "SqlAlchemySinkConfig",
]
