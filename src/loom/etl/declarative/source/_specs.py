"""Normalized source spec types — one frozen dataclass per source kind.

Internal module — import from :mod:`loom.etl.declarative.source`.

Each spec carries exactly the fields required by the compiler and backends.
Specs are produced by the builder ``_to_spec()`` methods and consumed by the
compiler and executor; they are never constructed directly in user code.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from loom.etl.declarative._format import Format
from loom.etl.declarative._read_options import ReadOptions
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.schema._schema import ColumnSchema, LoomType


class SourceKind(StrEnum):
    """Physical kind of an ETL source."""

    TABLE = "table"
    FILE = "file"
    TEMP = "temp"


@dataclass(frozen=True)
class JsonColumnSpec:
    """Declares that a string column contains JSON to be decoded at read time.

    Produced by :meth:`~loom.etl.declarative.source.FromTable.parse_json` and
    :meth:`~loom.etl.declarative.source.FromFile.parse_json`.
    Consumed by backend readers — never constructed directly in user code.

    Args:
        column:    Name of the string column that holds the JSON payload.
        loom_type: Target :data:`~loom.etl._schema.LoomType` to decode into.
    """

    column: str
    loom_type: LoomType


@dataclass(frozen=True)
class TableSourceSpec:
    """Normalized internal representation of a Delta table ETL source.

    Produced by :meth:`~loom.etl.declarative.source.FromTable._to_spec`.
    Consumed by the compiler and executor — never exposed in user code.

    Args:
        alias:        Name matching the ``execute()`` parameter.
        table_ref:    Logical table reference.
        predicates:   Compiled predicate nodes from ``.where()``.
        columns:      Column names to project at scan time.  When non-empty,
                      only these columns are read from storage — all other
                      columns are discarded before the frame reaches
                      ``execute()``.  The projection is pushed down to the
                      Parquet row-group scanner, reducing I/O.
        schema:       Optional user-declared schema applied at read time via
                      ``with_columns(cast(...))``; casts each declared column to
                      its :class:`~loom.etl._schema.LoomDtype`.  Extra columns
                      in the source pass through untouched.
        json_columns: JSON decode specs applied at read time.
    """

    alias: str
    table_ref: TableRef
    predicates: tuple[Any, ...] = field(default_factory=tuple)
    columns: tuple[str, ...] = field(default_factory=tuple)
    schema: tuple[ColumnSchema, ...] = field(default_factory=tuple)
    json_columns: tuple[JsonColumnSpec, ...] = field(default_factory=tuple)

    @property
    def kind(self) -> SourceKind:
        """Physical kind — always :attr:`SourceKind.TABLE`."""
        return SourceKind.TABLE

    @property
    def format(self) -> Format:
        """I/O format — always :attr:`Format.DELTA` for table sources."""
        return Format.DELTA


@dataclass(frozen=True)
class FileSourceSpec:
    """Normalized internal representation of a file-based ETL source.

    Produced by :meth:`~loom.etl.declarative.source.FromFile._to_spec`.
    Consumed by the compiler and executor — never exposed in user code.

    Args:
        alias:        Name matching the ``execute()`` parameter.
        path:         File path template resolved from params at runtime.
        format:       I/O format (CSV, JSON, XLSX, Parquet).
        read_options: Format-specific read options set via ``.with_options()``.
        columns:      Column names to project at scan time.  When non-empty,
                      only these columns are loaded from the file.
        schema:       Optional user-declared schema applied at read time via
                      column-level casts.
        json_columns: JSON decode specs applied at read time.
    """

    alias: str
    path: str
    format: Format
    read_options: ReadOptions | None = None
    columns: tuple[str, ...] = field(default_factory=tuple)
    schema: tuple[ColumnSchema, ...] = field(default_factory=tuple)
    json_columns: tuple[JsonColumnSpec, ...] = field(default_factory=tuple)

    @property
    def kind(self) -> SourceKind:
        """Physical kind — always :attr:`SourceKind.FILE`."""
        return SourceKind.FILE


@dataclass(frozen=True)
class TempSourceSpec:
    """Normalized internal representation of an intermediate (temp) ETL source.

    Produced by :meth:`~loom.etl.declarative.source.FromTemp._to_spec`.
    Consumed by the executor to retrieve data from
    :class:`~loom.etl.checkpoint.CheckpointStore`.

    Args:
        alias:     Name matching the ``execute()`` parameter.
        temp_name: Logical intermediate name matching :class:`~loom.etl.IntoTemp`.
    """

    alias: str
    temp_name: str

    @property
    def kind(self) -> SourceKind:
        """Physical kind — always :attr:`SourceKind.TEMP`."""
        return SourceKind.TEMP


# Type alias — the union of all typed source spec variants.
SourceSpec = TableSourceSpec | FileSourceSpec | TempSourceSpec
