"""TABLE target variant specs — one frozen dataclass per write semantic.

All variants carry exactly the fields required for their write mode.
Format is always ``Format.DELTA`` — implicit, not stored.

Internal module — import from :mod:`loom.etl.io.target`.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from loom.etl.io._target import SchemaMode
from loom.etl.schema._table import TableRef
from loom.etl.sql._predicate import PredicateNode


@dataclass(frozen=True)
class AppendSpec:
    """Append rows to an existing Delta table.

    Args:
        table_ref:   Logical table reference.
        schema_mode: Schema evolution strategy.  Defaults to ``STRICT``.
    """

    table_ref: TableRef
    schema_mode: SchemaMode = SchemaMode.STRICT


@dataclass(frozen=True)
class ReplaceSpec:
    """Full overwrite of a Delta table.

    Args:
        table_ref:   Logical table reference.
        schema_mode: Schema evolution strategy.  ``OVERWRITE`` replaces the
                     table schema alongside the data.
    """

    table_ref: TableRef
    schema_mode: SchemaMode = SchemaMode.STRICT


@dataclass(frozen=True)
class ReplacePartitionsSpec:
    """Overwrite only the partitions present in the source frame.

    The writer collects distinct partition-column combinations from the
    frame and builds the ``replaceWhere`` predicate at write time.

    Args:
        table_ref:      Logical table reference.
        partition_cols: Partition columns used to build the predicate.
        schema_mode:    Schema evolution strategy.
    """

    table_ref: TableRef
    partition_cols: tuple[str, ...]
    schema_mode: SchemaMode = SchemaMode.STRICT


@dataclass(frozen=True)
class ReplaceWhereSpec:
    """Overwrite rows matching a static predicate (Delta ``replaceWhere``).

    The predicate is resolved from run params at write time and passed
    directly to delta-rs.

    Args:
        table_ref:         Logical table reference.
        replace_predicate: Predicate node built with the col/params DSL.
        schema_mode:       Schema evolution strategy.
    """

    table_ref: TableRef
    replace_predicate: PredicateNode
    schema_mode: SchemaMode = SchemaMode.STRICT


@dataclass(frozen=True)
class UpsertSpec:
    """Merge rows into a Delta table using explicit merge keys (UPSERT).

    Args:
        table_ref:      Logical table reference.
        upsert_keys:    Columns that uniquely identify a row (MERGE ON).
        schema_mode:    Schema evolution strategy.
        partition_cols: Partition columns added to the MERGE ON predicate
                        for file-level pruning.  Strongly recommended for
                        large tables.
        upsert_exclude: Columns excluded from ``UPDATE SET`` on match.
                        Mutually exclusive with *upsert_include*.
        upsert_include: Explicit allow-list for ``UPDATE SET``.
                        Mutually exclusive with *upsert_exclude*.
    """

    table_ref: TableRef
    upsert_keys: tuple[str, ...]
    schema_mode: SchemaMode = SchemaMode.STRICT
    partition_cols: tuple[str, ...] = field(default_factory=tuple)
    upsert_exclude: tuple[str, ...] = field(default_factory=tuple)
    upsert_include: tuple[str, ...] = field(default_factory=tuple)
