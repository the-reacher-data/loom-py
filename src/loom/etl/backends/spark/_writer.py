"""SparkDeltaWriter — TargetWriter backed by PySpark + Delta Lake.

Two resolution modes controlled by the *locator* constructor argument:

* **Path-based** (``locator`` is a URI string, :class:`pathlib.Path`, or
  :class:`~loom.etl._locator.TableLocator`) — writes via
  ``DataFrameWriter.save(uri)``.  Works with any cloud storage that Spark
  can reach: S3, GCS, ADLS, DBFS, local.

* **Unity Catalog** (``locator=None``) — writes via
  ``DataFrameWriter.saveAsTable(ref.ref)``, delegating all path and
  credential resolution to the active Spark catalog.

See https://docs.databricks.com/en/data-governance/unity-catalog/index.html
"""

from __future__ import annotations

import logging
import os
from typing import Any

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, DataFrameWriter, SparkSession
from pyspark.sql import types as T

from loom.etl.backends.spark._schema import spark_apply_schema
from loom.etl.io.target import SchemaMode, TargetSpec
from loom.etl.io.target._table import (
    AppendSpec,
    ReplacePartitionsSpec,
    ReplaceSpec,
    ReplaceWhereSpec,
    UpsertSpec,
)
from loom.etl.schema._table import TableRef
from loom.etl.sql._predicate_sql import predicate_to_sql
from loom.etl.sql._upsert import (
    SOURCE_ALIAS,
    TARGET_ALIAS,
    _build_insert_values,
    _build_partition_predicate,
    _build_update_set,
    _build_upsert_predicate,
    _build_upsert_update_cols,
    _log_partition_combos,
    _UpsertLike,
    _warn_no_partition_cols,
)
from loom.etl.storage._locator import TableLocator, _as_locator

_log = logging.getLogger(__name__)


class SparkDeltaWriter:
    """Write ETL step results to Delta tables using PySpark + Delta Lake.

    Implements :class:`~loom.etl._io.TargetWriter`.

    Validates or evolves the frame schema via
    :func:`~loom.etl.backends.spark._schema.spark_apply_schema` before each
    write.  Delta Lake / Unity Catalog is the authoritative schema store —
    no explicit catalog update is needed after a successful write.

    OVERWRITE mode is the only mode allowed to create a new table from scratch.

    Args:
        spark:   Active :class:`pyspark.sql.SparkSession`.
        locator: How to resolve table references to a physical location.

                 * Pass a URI string, :class:`pathlib.Path`, or any
                   :class:`~loom.etl._locator.TableLocator` for path-based
                   writes — final call is ``DataFrameWriter.save(uri)``.
                 * Pass ``None`` (default) for Unity Catalog — final call is
                   ``DataFrameWriter.saveAsTable(ref.ref)``.

    Example::

        from loom.etl.backends.spark import SparkDeltaWriter

        # Unity Catalog (Databricks managed)
        writer = SparkDeltaWriter(spark, None)

        # Cloud path
        writer = SparkDeltaWriter(spark, "s3://my-lake/")
    """

    def __init__(
        self,
        spark: SparkSession,
        locator: str | os.PathLike[str] | TableLocator | None,
    ) -> None:
        self._spark = spark
        self._locator = _as_locator(locator) if locator is not None else None

    def write(
        self, frame: DataFrame, spec: TargetSpec, params_instance: Any, *, streaming: bool = False
    ) -> None:
        """Validate schema and write *frame* to the Delta target.

        Args:
            frame:           Spark DataFrame produced by the step's ``execute()``.
            spec:            Compiled target spec variant.
            params_instance: Concrete params for predicate resolution.
            streaming:       Ignored — Spark manages its own execution model.

        Raises:
            TypeError:           If *spec* is a FILE or TEMP target.
            SchemaNotFoundError: When the table has no registered schema and
                                 mode is not OVERWRITE.
            SchemaError:         When the frame violates the registered schema.
        """
        _ = streaming
        if isinstance(spec, UpsertSpec):
            self._write_upsert_spark(frame, spec)
            return
        if not isinstance(spec, (AppendSpec, ReplaceSpec, ReplacePartitionsSpec, ReplaceWhereSpec)):
            raise TypeError(f"SparkDeltaWriter only supports TABLE targets; got: {type(spec)!r}")

        table_ref = spec.table_ref
        existing_schema = _native_spark_schema(self._spark, table_ref, self._locator)

        if existing_schema is None and spec.schema_mode is SchemaMode.OVERWRITE:
            if isinstance(spec, ReplacePartitionsSpec):
                _first_run_overwrite_partitions_spark(
                    frame, spec.partition_cols, table_ref, self._locator
                )
                return
            self._write_frame(frame, spec, params_instance)
            return

        validated = spark_apply_schema(frame, existing_schema, spec.schema_mode)
        self._write_frame(validated, spec, params_instance)

    def append(
        self,
        frame: DataFrame,
        table_ref: TableRef,
        params_instance: Any,
        *,
        streaming: bool = False,
    ) -> None:
        """Append rows to *table_ref* and create the table on first write.

        Uses ``schema_mode=EVOLVE`` for append semantics and falls back to
        ``OVERWRITE`` only when the table does not yet exist.

        Args:
            frame: Spark DataFrame to append.
            table_ref: Logical destination table reference.
            params_instance: Concrete params for predicate resolution.
            streaming: Ignored — Spark manages its own execution model.
        """
        _ = streaming
        if _native_spark_schema(self._spark, table_ref, self._locator) is None:
            self.write(
                frame,
                ReplaceSpec(table_ref=table_ref, schema_mode=SchemaMode.OVERWRITE),
                params_instance,
            )
            return
        self.write(
            frame,
            AppendSpec(table_ref=table_ref, schema_mode=SchemaMode.EVOLVE),
            params_instance,
        )

    def _write_upsert_spark(self, frame: DataFrame, spec: UpsertSpec) -> None:
        table_ref = spec.table_ref
        existing_schema = _native_spark_schema(self._spark, table_ref, self._locator)

        if existing_schema is None:
            _log.debug("upsert spark first run — creating table=%s", table_ref.ref)
            _first_run_overwrite_spark(frame, table_ref, self._locator)
            return

        validated = spark_apply_schema(frame, existing_schema, spec.schema_mode)
        _merge_spark(self._spark, validated, spec, table_ref, self._locator)

    def _write_frame(
        self,
        df: DataFrame,
        spec: AppendSpec | ReplaceSpec | ReplacePartitionsSpec | ReplaceWhereSpec,
        params_instance: Any,
    ) -> None:
        match spec:
            case ReplacePartitionsSpec():
                _write_replace_partitions_spark(
                    df, spec.partition_cols, spec.table_ref, self._locator
                )
                return
        writer = df.write.format("delta").option("optimizeWrite", "true")
        match spec:
            case AppendSpec():
                writer = _apply_append(writer, spec)
            case ReplaceWhereSpec():
                writer = _apply_replace_where(writer, spec, params_instance)
            case ReplaceSpec():
                writer = _apply_overwrite(writer, spec)
        _sink(writer, spec.table_ref, self._locator)


def _native_spark_schema(
    spark: SparkSession,
    ref: TableRef,
    locator: TableLocator | None,
) -> T.StructType | None:
    """Return the native PySpark schema for the table at *ref*, or ``None``.

    Reads the schema directly from the live Spark catalog or Delta path — no
    LoomType conversion.  Returns ``None`` when the table does not yet exist.

    Args:
        spark:   Active SparkSession.
        ref:     Logical table reference.
        locator: Path-based locator for non-Unity-Catalog tables, or ``None``
                 for Unity Catalog (``saveAsTable`` mode).

    Returns:
        Native :class:`~pyspark.sql.types.StructType` reflecting on-disk column
        types, or ``None`` when the table has not been written yet.
    """
    try:
        if locator is None:
            return spark.table(ref.ref).schema
        loc = locator.locate(ref)
        return spark.read.format("delta").load(loc.uri).schema
    except AnalysisException:
        return None


def _sink(writer: DataFrameWriter, ref: TableRef, locator: TableLocator | None) -> None:
    """Finalise the write: ``save(uri)`` for path-based, ``saveAsTable`` for UC."""
    if locator is None:
        writer.saveAsTable(ref.ref)
    else:
        writer.save(locator.locate(ref).uri)


def _apply_append(writer: DataFrameWriter, spec: AppendSpec) -> DataFrameWriter:
    writer = writer.mode("append")
    if spec.schema_mode is SchemaMode.EVOLVE:
        writer = writer.option("mergeSchema", "true")
    return writer


def _write_replace_partitions_spark(
    df: DataFrame,
    partition_cols: tuple[str, ...],
    table_ref: TableRef,
    locator: TableLocator | None,
) -> None:
    """Write a replace-partitions operation using a single Spark action.

    Collects distinct partition combinations once, guards against an empty
    frame, then writes with ``replaceWhere``.  Avoids the double-job pattern
    of calling ``isEmpty()`` followed by a separate ``collect()``.

    Args:
        df:             Source Spark DataFrame (schema-validated).
        partition_cols: Columns that define partition boundaries.
        table_ref:      Logical table reference.
        locator:        Path-based locator, or ``None`` for Unity Catalog.
    """
    rows = df.select(*partition_cols).distinct().collect()
    if not rows:
        _log.warning("replace_partitions table=%s has 0 rows — nothing written", table_ref.ref)
        return
    predicate = _build_partition_predicate((row.asDict() for row in rows), partition_cols)
    writer = (
        df.sortWithinPartitions(*partition_cols)
        .write.format("delta")
        .option("optimizeWrite", "true")
        .mode("overwrite")
        .option("replaceWhere", predicate)
    )
    _sink(writer, table_ref, locator)


def _apply_replace_where(
    writer: DataFrameWriter, spec: ReplaceWhereSpec, params: Any
) -> DataFrameWriter:
    predicate = predicate_to_sql(spec.replace_predicate, params)
    return writer.mode("overwrite").option("replaceWhere", predicate)


def _apply_overwrite(writer: DataFrameWriter, spec: ReplaceSpec) -> DataFrameWriter:
    writer = writer.mode("overwrite")
    if spec.schema_mode is SchemaMode.OVERWRITE:
        writer = writer.option("overwriteSchema", "true")
    return writer


def _first_run_overwrite_spark(
    df: DataFrame,
    table_ref: TableRef,
    locator: TableLocator | None,
) -> None:
    """Create the Delta table on the first UPSERT run (no existing table).

    Uses a plain overwrite with ``overwriteSchema=true`` so the table and
    schema are initialised from the frame.  Subsequent runs use MERGE.

    Args:
        df:        Source Spark DataFrame.
        table_ref: Logical table reference.
        locator:   Path-based locator, or ``None`` for Unity Catalog.
    """
    writer = (
        df.write.format("delta")
        .option("optimizeWrite", "true")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )
    _sink(writer, table_ref, locator)


def _first_run_overwrite_partitions_spark(
    df: DataFrame,
    partition_cols: tuple[str, ...],
    table_ref: TableRef,
    locator: TableLocator | None,
) -> None:
    """Create a partitioned table on first write for ``replace_partitions``.

    Args:
        df:             Source Spark DataFrame.
        partition_cols: Columns to partition by.
        table_ref:      Logical table reference.
        locator:        Path-based locator, or ``None`` for Unity Catalog.
    """
    writer = (
        df.write.format("delta")
        .option("optimizeWrite", "true")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    _sink(writer, table_ref, locator)


def _collect_partition_combos_for_merge_spark(
    df: DataFrame,
    partition_cols: tuple[str, ...],
    table_ref: str,
) -> list[dict[str, Any]]:
    """Collect partition combos and emit observability signals for Spark.

    Emits a WARNING when no partition columns are declared (full table scan),
    and a DEBUG message with the combo count otherwise.  Partition columns have
    low cardinality by design so the collected result is always small.

    Args:
        df:             Source Spark DataFrame.
        partition_cols: Partition column names.
        table_ref:      Logical table reference for log messages.

    Returns:
        List of ``{col: value}`` dicts, one per distinct combination.
        Empty when no partition cols are declared.
    """
    if not partition_cols:
        _warn_no_partition_cols(table_ref)
        return []
    rows = df.select(*partition_cols).distinct().collect()
    combos = [{col: row[col] for col in partition_cols} for row in rows]
    _log_partition_combos(combos, table_ref)
    return combos


def _resolve_delta_table_spark(
    spark: SparkSession,
    table_ref: TableRef,
    locator: TableLocator | None,
) -> Any:
    """Resolve the Delta table handle for a MERGE operation.

    Uses ``forPath`` for path-based locators and ``forName`` for Unity Catalog.

    Args:
        spark:     Active SparkSession.
        table_ref: Logical table reference.
        locator:   Path-based locator, or ``None`` for Unity Catalog.

    Returns:
        A ``DeltaTable`` instance aliased to :data:`~loom.etl._upsert.TARGET_ALIAS`.
    """
    from delta.tables import DeltaTable

    if locator is not None:
        uri = locator.locate(table_ref).uri
        return DeltaTable.forPath(spark, uri).alias(TARGET_ALIAS)
    return DeltaTable.forName(spark, table_ref.ref).alias(TARGET_ALIAS)


def _merge_spark(
    spark: SparkSession,
    df: DataFrame,
    op: _UpsertLike,
    table_ref: TableRef,
    locator: TableLocator | None,
) -> None:
    """Execute a Delta MERGE (UPSERT) for the Spark backend.

    Args:
        spark:     Active SparkSession.
        df:        Source DataFrame (after schema validation).
        op:        Upsert configuration carrying keys, partition cols, exclude/include.
        table_ref: Logical table reference.
        locator:   Path-based locator, or ``None`` for Unity Catalog.
    """
    table_ref_str = table_ref.ref
    combos = _collect_partition_combos_for_merge_spark(df, op.partition_cols, table_ref_str)
    predicate = _build_upsert_predicate(combos, op, TARGET_ALIAS, SOURCE_ALIAS)
    update_cols = _build_upsert_update_cols(tuple(df.columns), op)
    update_set = _build_update_set(update_cols, SOURCE_ALIAS)
    insert_values = _build_insert_values(tuple(df.columns), SOURCE_ALIAS)

    dt = _resolve_delta_table_spark(spark, table_ref, locator)
    (
        dt.merge(df.alias(SOURCE_ALIAS), predicate)
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )
