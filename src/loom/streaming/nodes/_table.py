"""IntoTable streaming sink node — writes typed events to Delta or SQLAlchemy tables.

IntoTable is a frozen dataclass that implements IntoSink structurally.
The compiler detects it via isinstance(node, IntoSink) without any
registration or inheritance.

Supported backends
------------------
- ``Backend.SQLALCHEMY``: bulk-inserts rows using SQLAlchemy Core.
  Requires a ``url`` key in the resolved config section.
- ``Backend.DELTA``: appends a PyArrow batch via delta-rs.
  Requires a ``uri`` key in the resolved config section.
  Install extras: ``pip install deltalake polars``.

Config example (streaming.sinks.<name>)::

    streaming:
      sinks:
        orders_sink:
          url: postgresql+psycopg2://user:pass@host/db
          # table: orders  # overrides DSL table field when present

        events_sink:
          uri: s3://data-lake/events
          mode: append            # delta write mode (default: append)
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, ClassVar, Generic, Literal, TypeVar, cast

import msgspec
import msgspec.structs

from loom.streaming.core._message import StreamPayload
from loom.streaming.nodes._sink import SinkPartition

EventT = TypeVar("EventT", bound=StreamPayload)


class Backend(StrEnum):
    """Storage backend used by :class:`IntoTable` to write epoch batches.

    Args:
        SQLALCHEMY: Bulk-insert rows via SQLAlchemy Core. Transport-agnostic —
            works with any SA-supported database.
        DELTA:      Append a PyArrow batch to a Delta Lake table. Requires
            ``deltalake`` and ``polars`` to be installed.
    """

    SQLALCHEMY = "sqlalchemy"
    DELTA = "delta"


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
    table: str
    backend: Backend = Backend.SQLALCHEMY
    name: str = ""
    router_branch_safe: ClassVar[bool] = True

    def build_partition(
        self,
        config: Mapping[str, Any],
        worker_index: int,
        worker_count: int,
    ) -> SinkPartition[EventT]:
        """Build the per-worker partition for this sink.

        Args:
            config:       Resolved ``streaming.sinks.<name>`` config section.
            worker_index: Zero-based index of the calling worker (unused).
            worker_count: Total number of workers (unused).

        Returns:
            A :class:`~loom.streaming.nodes.SinkPartition` ready to receive
            ``write_batch`` and ``close`` calls.

        Raises:
            ValueError: If the resolved backend is not supported.
        """
        del worker_index, worker_count
        table_name = str(config.get("table") or self.table)
        if self.backend is Backend.SQLALCHEMY:
            return cast(
                SinkPartition[EventT],
                _SQLAlchemyTablePartition(table_name, self.payload, config),
            )
        if self.backend is Backend.DELTA:
            return cast(
                SinkPartition[EventT],
                _DeltaTablePartition(table_name, self.payload, config),
            )
        raise ValueError(f"Unsupported backend: {self.backend!r}")


class _SQLAlchemyTablePartition:
    """Bulk-insert epoch batches into a relational table via SQLAlchemy Core.

    Opens one connection per ``write_batch`` call and commits atomically.
    The table schema is inferred from the msgspec Struct field names —
    no ORM mapping or reflection required.

    Args:
        table_name:   Target table name.
        payload_type: Struct type whose fields map to table columns.
        config:       Resolved sink config.  Must contain ``url``.

    Raises:
        ValueError: If ``url`` is absent from *config*.
        ImportError: If SQLAlchemy is not installed.
    """

    def __init__(
        self,
        table_name: str,
        payload_type: type[Any],
        config: Mapping[str, Any],
    ) -> None:
        try:
            from sqlalchemy import Column, MetaData, Table, create_engine
        except ImportError as exc:
            raise ImportError(
                "SQLAlchemy backend requires 'sqlalchemy'. Install it with: pip install sqlalchemy"
            ) from exc

        url = config.get("url") or config.get("connection_string")
        if not url:
            raise ValueError(
                f"SQLAlchemy sink for table '{table_name}' requires a 'url' in config."
            )
        self._engine = create_engine(str(url))
        fields = msgspec.structs.fields(payload_type)
        metadata = MetaData()
        self._table = Table(table_name, metadata, *[Column(f.name) for f in fields])

    def write_batch(self, items: Sequence[Any]) -> None:
        """Bulk-insert one epoch batch.

        Args:
            items: Struct instances to insert.  Empty batches are skipped.
        """
        if not items:
            return
        rows = [msgspec.structs.asdict(cast(msgspec.Struct, item)) for item in items]
        with self._engine.begin() as conn:
            conn.execute(self._table.insert(), rows)

    def close(self) -> None:
        """Dispose the engine and release connection-pool resources."""
        self._engine.dispose()


_DELTA_MODES: frozenset[str] = frozenset({"error", "append", "ignore"})


def _validate_delta_mode(value: object) -> Literal["error", "append", "ignore"]:
    mode = str(value)
    if mode not in _DELTA_MODES:
        raise ValueError(
            f"Invalid Delta write mode {mode!r}. Must be one of: {sorted(_DELTA_MODES)}"
        )
    return mode  # type: ignore[return-value]


class _DeltaTablePartition:
    """Append epoch batches to a Delta Lake table via delta-rs and Polars.

    Converts each struct to a Polars DataFrame and calls
    ``write_deltalake`` with the configured write mode.

    Args:
        table_uri:    Delta table path or URI (``s3://…``, ``/local/…``).
        payload_type: Struct type used for field introspection.
        config:       Resolved sink config.  Must contain ``uri``.

    Raises:
        ValueError: If ``uri`` is absent from *config*.
        ImportError: If ``deltalake`` or ``polars`` are not installed.
    """

    def __init__(
        self,
        table_uri: str,
        payload_type: type[Any],
        config: Mapping[str, Any],
    ) -> None:
        try:
            import deltalake  # noqa: F401
            import polars  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "Delta backend requires 'deltalake' and 'polars'. "
                "Install them with: pip install deltalake polars"
            ) from exc

        uri = config.get("uri") or table_uri
        if not uri:
            raise ValueError(f"Delta sink for table '{table_uri}' requires a 'uri' in config.")
        self._uri = str(uri)
        self._mode: Literal["error", "append", "ignore"] = _validate_delta_mode(
            config.get("mode", "append")
        )
        self._storage_options: dict[str, str] = {
            k: str(v) for k, v in (config.get("storage_options") or {}).items()
        }

    def write_batch(self, items: Sequence[Any]) -> None:
        """Convert structs to a Polars DataFrame and write to Delta.

        Args:
            items: Struct instances to write.  Empty batches are skipped.
        """
        if not items:
            return
        import polars as pl
        from deltalake.writer import write_deltalake

        rows = [msgspec.structs.asdict(cast(msgspec.Struct, item)) for item in items]
        df = pl.DataFrame(rows)
        write_deltalake(
            self._uri,
            df,
            mode=self._mode,
            storage_options=self._storage_options or None,
        )

    def close(self) -> None:
        """No-op — delta-rs holds no persistent connection."""


__all__ = ["Backend", "IntoTable"]
