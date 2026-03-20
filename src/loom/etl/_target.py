"""ETL target declaration types.

Public API:  ``IntoTable``, ``IntoFile``, ``SchemaMode``.
Internal:    ``TargetSpec``, ``WriteMode`` — used by the compiler only.

Each ETL step declares exactly one target.  Write mode and schema mode are
set by passing keyword arguments to the write-intent method::

    IntoTable("staging.orders").append()
    IntoTable("staging.orders").replace()
    IntoTable("staging.orders").replace(schema=SchemaMode.EVOLVE)
    IntoTable("staging.orders").replace(schema=SchemaMode.OVERWRITE)
    IntoTable("staging.orders").partition_replace(by=params.run_date)
    IntoTable("staging.orders").partition_replace(
        by=params.run_date, schema=SchemaMode.EVOLVE
    )
    IntoTable("staging.orders").upsert(keys=("order_id",))
    IntoFile("s3://exports/report_{run_date}.csv", format=Format.CSV)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from loom.etl._format import Format
from loom.etl._table import TableRef


class WriteMode(StrEnum):
    """Supported write semantics for ETL targets."""

    APPEND = "append"
    REPLACE = "replace"
    PARTITION_REPLACE = "partition_replace"
    UPSERT = "upsert"


class SchemaMode(StrEnum):
    """Schema evolution strategy applied by the target writer before each write.

    Declared alongside the write mode on :class:`IntoTable`::

        target = IntoTable("staging.orders").replace(schema=SchemaMode.EVOLVE)

    Values:

    * ``STRICT``    — default; fails if the frame's types are incompatible with
                      the existing table schema.  Safest option.
    * ``EVOLVE``    — adds columns present in the frame but absent in the table
                      schema (``mergeSchema`` in Delta).  Existing columns are
                      still validated for type compatibility.
    * ``OVERWRITE`` — replaces the table schema with the frame's schema.
                      Only valid with :attr:`~WriteMode.REPLACE`.  Use with care.
    """

    STRICT = "strict"
    EVOLVE = "evolve"
    OVERWRITE = "overwrite"


@dataclass(frozen=True)
class TargetSpec:
    """Normalized internal representation of one ETL target.

    Produced by :meth:`IntoTable._to_spec` and :meth:`IntoFile._to_spec`.
    Consumed by the compiler and executor — never exposed in user code.

    Args:
        mode:        Write semantics.
        format:      I/O format.
        schema_mode: Schema evolution strategy.
        table_ref:   Logical table reference (table targets only).
        path:        File path template (file targets only).
        partition_by: Param expression for ``partition_replace`` mode.
        upsert_keys:  Column names used as merge keys for ``upsert`` mode.
    """

    mode: WriteMode
    format: Format
    schema_mode: SchemaMode = SchemaMode.STRICT
    table_ref: TableRef | None = None
    path: str | None = None
    partition_by: Any = None
    upsert_keys: tuple[str, ...] = field(default_factory=tuple)


class IntoTable:
    """Declare a Delta table as the ETL step target.

    The write mode is set by chaining one of:
    :meth:`append`, :meth:`replace`, :meth:`partition_replace`,
    :meth:`upsert`.  Defaults to :attr:`~WriteMode.REPLACE` if no mode
    is chained (explicit is still preferred).

    Args:
        ref: Logical table reference — ``str`` or :class:`~loom.etl.TableRef`.

    Example::

        target = IntoTable("staging.orders").partition_replace(by=params.run_date)
    """

    __slots__ = ("_ref", "_spec")

    def __init__(self, ref: str | TableRef) -> None:
        table_ref = TableRef(ref) if isinstance(ref, str) else ref
        self._ref = table_ref
        self._spec = TargetSpec(
            mode=WriteMode.REPLACE,
            format=Format.DELTA,
            table_ref=table_ref,
        )

    def append(self, *, schema: SchemaMode = SchemaMode.STRICT) -> IntoTable:
        """Write mode: append rows to the target table.

        Args:
            schema: Schema evolution strategy.  Defaults to
                    :attr:`~SchemaMode.STRICT`.

        Returns:
            New ``IntoTable`` with :attr:`~WriteMode.APPEND` mode.
        """
        return self._with(mode=WriteMode.APPEND, schema_mode=schema)

    def replace(self, *, schema: SchemaMode = SchemaMode.STRICT) -> IntoTable:
        """Write mode: full replace of the target table.

        Args:
            schema: Schema evolution strategy.  Use
                    :attr:`~SchemaMode.OVERWRITE` to replace the table schema
                    entirely alongside the data.

        Returns:
            New ``IntoTable`` with :attr:`~WriteMode.REPLACE` mode.
        """
        return self._with(mode=WriteMode.REPLACE, schema_mode=schema)

    def partition_replace(self, *, by: Any, schema: SchemaMode = SchemaMode.STRICT) -> IntoTable:
        """Write mode: replace only the partitions covered by ``by``.

        Args:
            by:     Partition key — a :class:`~loom.etl._proxy.ParamExpr` or
                    a :class:`~loom.etl._table.ColumnRef`.
            schema: Schema evolution strategy.

        Returns:
            New ``IntoTable`` with :attr:`~WriteMode.PARTITION_REPLACE` mode.
        """
        return self._with(mode=WriteMode.PARTITION_REPLACE, partition_by=by, schema_mode=schema)

    def upsert(self, *, keys: tuple[str, ...], schema: SchemaMode = SchemaMode.STRICT) -> IntoTable:
        """Write mode: merge rows using the given key columns.

        Args:
            keys:   Tuple of column names that identify a row uniquely.
            schema: Schema evolution strategy.

        Returns:
            New ``IntoTable`` with :attr:`~WriteMode.UPSERT` mode.
        """
        return self._with(mode=WriteMode.UPSERT, upsert_keys=keys, schema_mode=schema)

    def _with(self, **overrides: Any) -> IntoTable:
        new = object.__new__(IntoTable)
        current = {
            "mode": self._spec.mode,
            "format": self._spec.format,
            "schema_mode": self._spec.schema_mode,
            "table_ref": self._spec.table_ref,
            "partition_by": self._spec.partition_by,
            "upsert_keys": self._spec.upsert_keys,
        }
        current.update(overrides)
        object.__setattr__(new, "_ref", self._ref)
        object.__setattr__(new, "_spec", TargetSpec(**current))
        return new

    def _to_spec(self) -> TargetSpec:
        return self._spec

    def __repr__(self) -> str:
        return f"IntoTable({self._ref.ref!r}, mode={self._spec.mode!r})"


class IntoFile:
    """Declare a file as the ETL step target (CSV, JSON, XLSX, Parquet).

    The ``path`` supports ``{field_name}`` template placeholders resolved
    from params at runtime.

    Args:
        path:   File path or template, e.g. ``"s3://exports/orders_{run_date}.csv"``.
        format: :class:`~loom.etl.Format` of the output file.

    Example::

        target = IntoFile("s3://exports/summary_{run_date}.xlsx", format=Format.XLSX)
    """

    __slots__ = ("_path", "_format")

    def __init__(self, path: str, *, format: Format) -> None:
        self._path = path
        self._format = format

    def _to_spec(self) -> TargetSpec:
        return TargetSpec(
            mode=WriteMode.REPLACE,
            format=self._format,
            path=self._path,
        )

    def __repr__(self) -> str:
        return f"IntoFile({self._path!r}, format={self._format!r})"
