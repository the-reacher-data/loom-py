"""ETL target declaration types.

Public API:  ``IntoTable``, ``IntoFile``, ``SchemaMode``.
Internal:    ``TargetSpec``, ``WriteMode`` — used by the compiler only.

Each ETL step declares exactly one target.  Write mode and schema mode are
set by chaining a write-intent method::

    IntoTable("staging.orders").append()
    IntoTable("staging.orders").replace()
    IntoTable("staging.orders").replace(schema=SchemaMode.EVOLVE)
    IntoTable("staging.orders").replace(schema=SchemaMode.OVERWRITE)
    IntoTable("staging.orders").replace_partitions("year", "month")
    IntoTable("staging.orders").replace_where(
        (col("year") == params.run_date.year) & (col("month") == params.run_date.month)
    )
    IntoTable("staging.orders").upsert(keys=("order_id",))
    IntoFile("s3://exports/report_{run_date}.csv", format=Format.CSV)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from dataclasses import replace as _dc_replace
from enum import StrEnum
from typing import Any

from loom.etl._format import Format
from loom.etl._predicate import AndPred, EqPred, PredicateNode
from loom.etl._proxy import ParamExpr
from loom.etl._table import TableRef, UnboundColumnRef
from loom.etl._temp import TempScope
from loom.etl._write_options import WriteOptions


class WriteMode(StrEnum):
    """Supported write semantics for ETL targets."""

    APPEND = "append"
    REPLACE = "replace"
    REPLACE_PARTITIONS = "replace_partitions"
    REPLACE_WHERE = "replace_where"
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
        mode:             Write semantics.
        format:           I/O format.
        schema_mode:      Schema evolution strategy.
        table_ref:        Logical table reference (table targets only).
        path:             File path template (file targets only).
        partition_cols:   Column names for :attr:`~WriteMode.REPLACE_PARTITIONS`.
        replace_predicate: Predicate node for :attr:`~WriteMode.REPLACE_WHERE`.
        upsert_keys:      Column names used as merge keys for ``upsert`` mode.
    """

    mode: WriteMode
    format: Format
    schema_mode: SchemaMode = SchemaMode.STRICT
    table_ref: TableRef | None = None
    path: str | None = None
    partition_cols: tuple[str, ...] = field(default_factory=tuple)
    replace_predicate: PredicateNode | None = None
    upsert_keys: tuple[str, ...] = field(default_factory=tuple)
    upsert_exclude: tuple[str, ...] = field(default_factory=tuple)
    upsert_include: tuple[str, ...] = field(default_factory=tuple)
    temp_name: str | None = None
    temp_scope: TempScope | None = None
    write_options: WriteOptions | None = None


def _build_eq_predicate(values: dict[str, ParamExpr]) -> PredicateNode:
    items = list(values.items())
    head_col, head_expr = items[0]
    result: PredicateNode = EqPred(left=UnboundColumnRef(head_col), right=head_expr)
    for col_name, expr in items[1:]:
        result = AndPred(left=result, right=EqPred(left=UnboundColumnRef(col_name), right=expr))
    return result


class IntoTable:
    """Declare a Delta table as the ETL step target.

    The write mode is set by chaining one of:
    :meth:`append`, :meth:`replace`, :meth:`replace_partitions`,
    :meth:`replace_where`, :meth:`upsert`.

    Args:
        ref: Logical table reference — ``str`` or :class:`~loom.etl.TableRef`.

    Example::

        # Full replace
        target = IntoTable("staging.orders").replace()

        # Replace only the partitions present in the batch
        target = IntoTable("staging.orders").replace_partitions("year", "month")

        # Replace a date-range for backfill
        target = IntoTable("staging.orders").replace_where(
            col("date").between(params.start_date, params.end_date)
        )
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

        Overwrites all data in the table.  Use :meth:`replace_partitions` for
        partition-scoped overwrite or :meth:`replace_where` for predicate-scoped.

        Args:
            schema: Schema evolution strategy.  Use
                    :attr:`~SchemaMode.OVERWRITE` to replace the table schema
                    entirely alongside the data.

        Returns:
            New ``IntoTable`` with :attr:`~WriteMode.REPLACE` mode.
        """
        return self._with(mode=WriteMode.REPLACE, schema_mode=schema)

    def replace_partitions(
        self,
        *cols: str,
        values: dict[str, ParamExpr] | None = None,
        schema: SchemaMode = SchemaMode.STRICT,
    ) -> IntoTable:
        """Write mode: replace only the relevant partitions.

        Two calling styles — pass one or the other, never both:

        * **Dynamic** — positional column names.  The writer collects the
          distinct partition values from the batch and builds the predicate
          at write time::

              target = IntoTable("staging.orders").replace_partitions("year", "month")

        * **From params** — ``values`` dict maps column name to a
          :class:`~loom.etl._proxy.ParamExpr`.  The predicate is resolved from
          the run params; no collect required::

              target = IntoTable("staging.orders").replace_partitions(
                  values={"year": params.run_date.year, "month": params.run_date.month}
              )

        Args:
            *cols:   Partition column names (dynamic style).
            values:  Mapping of column name → :class:`~loom.etl._proxy.ParamExpr`
                     (params style).  Keys become the partition column names.
            schema:  Schema evolution strategy.

        Returns:
            New ``IntoTable`` instance.

        Raises:
            ValueError: If both *cols* and *values* are supplied, or neither is.
        """
        if cols and values is not None:
            raise ValueError(
                "replace_partitions: pass either positional column names or values=, not both"
            )
        if not cols and values is None:
            raise ValueError("replace_partitions: pass column names or values=")

        if values is not None:
            predicate = _build_eq_predicate(values)
            return self._with(
                mode=WriteMode.REPLACE_WHERE,
                replace_predicate=predicate,
                partition_cols=tuple(values.keys()),
                schema_mode=schema,
            )

        return self._with(
            mode=WriteMode.REPLACE_PARTITIONS,
            partition_cols=cols,
            schema_mode=schema,
        )

    def replace_where(
        self,
        predicate: PredicateNode,
        *,
        schema: SchemaMode = SchemaMode.STRICT,
    ) -> IntoTable:
        """Write mode: replace rows matching an explicit predicate.

        The predicate is resolved against the run params and passed to Delta as
        ``replaceWhere``.  Only the matching data is overwritten — Delta uses
        partition pruning so only affected files are rewritten.

        Typical use case: backfill / reprocessing a date range.

        Args:
            predicate: Predicate built with the :func:`~loom.etl.col` /
                       :data:`~loom.etl.params` DSL.
            schema:    Schema evolution strategy.

        Returns:
            New ``IntoTable`` with :attr:`~WriteMode.REPLACE_WHERE` mode.

        Example::

            target = IntoTable("staging.orders").replace_where(
                col("date").between(params.start_date, params.end_date)
            )
        """
        return self._with(
            mode=WriteMode.REPLACE_WHERE,
            replace_predicate=predicate,
            schema_mode=schema,
        )

    def upsert(
        self,
        *,
        keys: tuple[str, ...],
        partition_cols: tuple[str, ...] = (),
        exclude: tuple[str, ...] = (),
        include: tuple[str, ...] = (),
        schema: SchemaMode = SchemaMode.STRICT,
    ) -> IntoTable:
        """Write mode: merge rows using the given key columns (UPSERT / MERGE).

        On first write the table is created from the frame.  Subsequent writes
        issue a Delta MERGE: matched rows are updated, unmatched rows are
        inserted.

        Declaring ``partition_cols`` is strongly recommended for large tables —
        it allows Delta to prune files at the log level before evaluating the
        join condition.  Without it, every MERGE forces a full table scan.

        Args:
            keys:           Columns that uniquely identify a row.  Used in the
                            MERGE ``ON`` join condition.
            partition_cols: Partition columns to include in the MERGE ``ON``
                            predicate and for Delta log pruning.  Must be a
                            subset of the frame columns.
            exclude:        Columns to exclude from ``UPDATE SET`` on match.
                            Keys and partition columns are always excluded.
                            Mutually exclusive with *include*.
            include:        Explicit allow-list of columns to update on match.
                            Keys and partition columns are always excluded even
                            if listed here.  Mutually exclusive with *exclude*.
            schema:         Schema evolution strategy.  Defaults to
                            :attr:`~SchemaMode.STRICT`.

        Returns:
            New ``IntoTable`` with :attr:`~WriteMode.UPSERT` mode.

        Example::

            target = IntoTable("events.orders").upsert(
                keys=("order_id",),
                partition_cols=("year", "month"),
                exclude=("created_at",),
            )
        """
        return self._with(
            mode=WriteMode.UPSERT,
            upsert_keys=keys,
            partition_cols=partition_cols,
            upsert_exclude=exclude,
            upsert_include=include,
            schema_mode=schema,
        )

    def _with(self, **overrides: Any) -> IntoTable:
        new = object.__new__(IntoTable)
        object.__setattr__(new, "_ref", self._ref)
        object.__setattr__(new, "_spec", _dc_replace(self._spec, **overrides))  # pyright: ignore[reportArgumentType]
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

    __slots__ = ("_path", "_format", "_write_options")

    def __init__(self, path: str, *, format: Format) -> None:
        self._path = path
        self._format = format
        self._write_options: WriteOptions | None = None

    def with_options(self, options: WriteOptions) -> IntoFile:
        """Return a new ``IntoFile`` with format-specific write options.

        Args:
            options: Format-specific write options — use
                     :class:`~loom.etl.CsvWriteOptions` or
                     :class:`~loom.etl.ParquetWriteOptions`.

        Returns:
            New ``IntoFile`` with the options applied at write time.

        Example::

            target = IntoFile("s3://exports/report.csv", format=Format.CSV)
                .with_options(CsvWriteOptions(separator=";"))
        """
        new = object.__new__(IntoFile)
        object.__setattr__(new, "_path", self._path)
        object.__setattr__(new, "_format", self._format)
        object.__setattr__(new, "_write_options", options)
        return new

    def _to_spec(self) -> TargetSpec:
        return TargetSpec(
            mode=WriteMode.REPLACE,
            format=self._format,
            path=self._path,
            write_options=self._write_options,
        )

    def __repr__(self) -> str:
        return f"IntoFile({self._path!r}, format={self._format!r})"


class IntoTemp:
    """Declare an intermediate result that bypasses Delta and lives in tmp storage.

    The physical format is chosen automatically by
    :class:`~loom.etl._temp_store.IntermediateStore` based on the DataFrame
    type returned by ``execute()``:

    * **Polars** — Arrow IPC via ``sink_ipc()`` (streaming write, no collect)
      and ``scan_ipc()`` (lazy, memory-mapped read with predicate pushdown).
    * **Spark** — Parquet directory via ``df.write.parquet()`` and
      ``spark.read.parquet()``.  Cuts the lineage DAG; Photon-optimised.

    Use :class:`~loom.etl.FromTemp` in a downstream step to consume the result.

    Args:
        name:  Logical name identifying this intermediate.  Must be unique
               within the pipeline.  Downstream steps reference it by this
               name via :class:`~loom.etl.FromTemp`.
        scope: Lifetime scope.  Defaults to :attr:`~loom.etl.TempScope.RUN`.

    Example::

        target = IntoTemp("normalized_orders")
        target = IntoTemp("normalized_orders", scope=TempScope.CORRELATION)
    """

    __slots__ = ("_name", "_scope")

    def __init__(self, name: str, *, scope: TempScope = TempScope.RUN) -> None:
        self._name = name
        self._scope = scope

    @property
    def temp_name(self) -> str:
        """Logical name of this intermediate."""
        return self._name

    @property
    def scope(self) -> TempScope:
        """Lifetime scope."""
        return self._scope

    def _to_spec(self) -> TargetSpec:
        return TargetSpec(
            mode=WriteMode.REPLACE,
            format=Format.PARQUET,
            temp_name=self._name,
            temp_scope=self._scope,
        )

    def __repr__(self) -> str:
        return f"IntoTemp({self._name!r}, scope={self._scope!r})"
