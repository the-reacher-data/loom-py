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

from pyspark.sql import DataFrame, SparkSession

from loom.etl.backends.spark._dtype import spark_to_loom
from loom.etl.backends.spark._schema import spark_apply_schema
from loom.etl.io._target import SchemaMode, TargetSpec, WriteMode
from loom.etl.schema._schema import ColumnSchema
from loom.etl.schema._table import TableRef
from loom.etl.sql._predicate_sql import predicate_to_sql
from loom.etl.sql._upsert import (
    SOURCE_ALIAS,
    TARGET_ALIAS,
    _build_partition_predicate,
    _build_update_set,
    _build_upsert_predicate,
    _build_upsert_update_cols,
    _log_partition_combos,
    _warn_no_partition_cols,
)
from loom.etl.storage._io import TableDiscovery
from loom.etl.storage._locator import TableLocator, _as_locator

_log = logging.getLogger(__name__)


class SparkDeltaWriter:
    """Write ETL step results to Delta tables using PySpark + Delta Lake.

    Implements :class:`~loom.etl._io.TargetWriter`.

    Validates or evolves the frame schema via
    :func:`~loom.etl.backends.spark._schema.spark_apply_schema` before each
    write, then updates the catalog so later steps see the evolved schema.

    OVERWRITE mode is the only mode allowed to create a new table from scratch.

    Args:
        spark:   Active :class:`pyspark.sql.SparkSession`.
        locator: How to resolve table references to a physical location.

                 * Pass a URI string, :class:`pathlib.Path`, or any
                   :class:`~loom.etl._locator.TableLocator` for path-based
                   writes — final call is ``DataFrameWriter.save(uri)``.
                 * Pass ``None`` (default) for Unity Catalog — final call is
                   ``DataFrameWriter.saveAsTable(ref.ref)``.
        catalog: Catalog used for schema lookup and post-write update.

    Example::

        from loom.etl.backends.spark import SparkDeltaWriter

        # Unity Catalog (Databricks managed)
        writer = SparkDeltaWriter(spark, None, catalog)

        # Cloud path
        writer = SparkDeltaWriter(spark, "s3://my-lake/", catalog)
    """

    def __init__(
        self,
        spark: SparkSession,
        locator: str | os.PathLike[str] | TableLocator | None,
        catalog: TableDiscovery,
    ) -> None:
        self._spark = spark
        self._locator = _as_locator(locator) if locator is not None else None
        self._catalog = catalog

    def write(self, frame: DataFrame, spec: TargetSpec, params_instance: Any) -> None:
        """Validate schema and write *frame* to the Delta target.

        Args:
            frame:           Spark DataFrame produced by the step's ``execute()``.
            spec:            Compiled target spec.
            params_instance: Concrete params for predicate resolution.

        Raises:
            TypeError:           If *spec* is a FILE target.
            SchemaNotFoundError: When the table has no registered schema and
                                 mode is not OVERWRITE.
            SchemaError:         When the frame violates the registered schema.
        """
        if spec.table_ref is None:
            raise TypeError(f"SparkDeltaWriter only supports TABLE targets; got FILE spec: {spec}")
        table_ref = spec.table_ref
        existing_schema = self._catalog.schema(table_ref)

        if spec.mode is WriteMode.UPSERT:
            self._write_upsert_spark(frame, spec, existing_schema)
            return

        if existing_schema is None and spec.schema_mode is SchemaMode.OVERWRITE:
            self._write_frame(frame, spec, params_instance)
            self._register_schema(table_ref, frame)
            return

        validated = spark_apply_schema(frame, existing_schema, spec.schema_mode)
        self._write_frame(validated, spec, params_instance)
        self._register_schema(table_ref, validated)

    def _write_upsert_spark(
        self,
        frame: DataFrame,
        spec: TargetSpec,
        existing_schema: Any,
    ) -> None:
        table_ref = spec.table_ref
        if table_ref is None:
            raise TypeError("table_ref must be set for UPSERT write operations")
        if existing_schema is None:
            _log.debug("upsert spark first run — creating table=%s", table_ref.ref)
            _first_run_overwrite_spark(self._spark, frame, spec, self._locator)
            self._register_schema(table_ref, frame)
            return

        validated = spark_apply_schema(frame, existing_schema, spec.schema_mode)
        _merge_spark(self._spark, validated, spec, self._locator)
        self._register_schema(table_ref, validated)

    def _write_frame(self, df: DataFrame, spec: TargetSpec, params_instance: Any) -> None:
        if spec.table_ref is None:
            raise TypeError("table_ref must be set for Delta write operations")
        df = _sort_for_write(df, spec)
        writer = df.write.format("delta").option("optimizeWrite", "true")
        writer = _MODE_APPLIERS[spec.mode](writer, df, spec, params_instance)
        _sink(writer, spec.table_ref, self._locator)

    def _register_schema(self, ref: TableRef, frame: DataFrame) -> None:
        schema = tuple(
            ColumnSchema(name=f.name, dtype=spark_to_loom(f.dataType)) for f in frame.schema.fields
        )
        self._catalog.update_schema(ref, schema)


def _sink(writer: Any, ref: TableRef, locator: TableLocator | None) -> None:
    """Finalise the write: ``save(uri)`` for path-based, ``saveAsTable`` for UC."""
    if locator is None:
        writer.saveAsTable(ref.ref)
    else:
        writer.save(locator.locate(ref).uri)


def _apply_append(writer: Any, _df: DataFrame, spec: TargetSpec, _params: Any) -> Any:
    writer = writer.mode("append")
    if spec.schema_mode is SchemaMode.EVOLVE:
        writer = writer.option("mergeSchema", "true")
    return writer


def _apply_replace_partitions(writer: Any, df: DataFrame, spec: TargetSpec, _params: Any) -> Any:
    predicate = _build_partition_predicate(
        (row.asDict() for row in df.select(*spec.partition_cols).distinct().collect()),
        spec.partition_cols,
    )
    return writer.mode("overwrite").option("replaceWhere", predicate)


def _apply_replace_where(writer: Any, _df: DataFrame, spec: TargetSpec, params: Any) -> Any:
    if spec.replace_predicate is None:
        raise TypeError("replace_predicate must be set for REPLACE_WHERE write mode")
    predicate = predicate_to_sql(spec.replace_predicate, params)
    return writer.mode("overwrite").option("replaceWhere", predicate)


def _apply_overwrite(writer: Any, _df: DataFrame, spec: TargetSpec, _params: Any) -> Any:
    writer = writer.mode("overwrite")
    if spec.schema_mode is SchemaMode.OVERWRITE:
        writer = writer.option("overwriteSchema", "true")
    return writer


_MODE_APPLIERS: dict[WriteMode, Any] = {
    WriteMode.APPEND: _apply_append,
    WriteMode.REPLACE_PARTITIONS: _apply_replace_partitions,
    WriteMode.REPLACE_WHERE: _apply_replace_where,
    WriteMode.REPLACE: _apply_overwrite,
}


def _first_run_overwrite_spark(
    _spark: SparkSession,
    df: DataFrame,
    spec: TargetSpec,
    locator: TableLocator | None,
) -> None:
    """Create the Delta table on the first UPSERT run (no existing table).

    Uses a plain overwrite with ``overwriteSchema=true`` so the table and
    schema are initialised from the frame.  Subsequent runs use MERGE.
    """
    if spec.table_ref is None:
        raise TypeError("table_ref must be set for first-run overwrite operations")
    writer = (
        df.write.format("delta")
        .option("optimizeWrite", "true")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )
    _sink(writer, spec.table_ref, locator)


def _collect_partition_combos_spark(
    df: DataFrame,
    partition_cols: tuple[str, ...],
) -> list[Any]:
    """Collect distinct partition value combinations from the source frame.

    Uses ``df.select(…).distinct().collect()``.  Partition columns have low
    cardinality by design so the collected result is always small.

    Args:
        df:             Source Spark DataFrame.
        partition_cols: Partition column names.

    Returns:
        List of Spark ``Row`` objects, one per distinct partition combination.
    """
    return df.select(*partition_cols).distinct().collect()


def _rows_to_combo_dicts(
    rows: list[Any],
    partition_cols: tuple[str, ...],
) -> list[dict[str, Any]]:
    """Convert collected Spark ``Row`` objects to plain dicts for SQL building.

    Args:
        rows:           Collected Spark rows.
        partition_cols: Column names to extract.

    Returns:
        List of ``{col: value}`` dicts.
    """
    return [{col: row[col] for col in partition_cols} for row in rows]


def _collect_partition_combos_for_merge_spark(
    df: DataFrame,
    partition_cols: tuple[str, ...],
    table_ref: str,
) -> list[dict[str, Any]]:
    """Collect partition combos and emit observability signals for Spark.

    Emits a WARNING when no partition columns are declared (full table scan),
    and a DEBUG message with the combo count otherwise.

    Args:
        df:             Source Spark DataFrame.
        partition_cols: Partition column names.
        table_ref:      Logical table reference for log messages.

    Returns:
        List of partition combination dicts (empty when no partition cols).
    """
    if not partition_cols:
        _warn_no_partition_cols(table_ref)
        return []
    rows = _collect_partition_combos_spark(df, partition_cols)
    combos = _rows_to_combo_dicts(rows, partition_cols)
    _log_partition_combos(combos, table_ref)
    return combos


def _resolve_delta_table_spark(
    spark: SparkSession,
    spec: TargetSpec,
    locator: TableLocator | None,
) -> Any:
    """Resolve the Delta table handle for a MERGE operation.

    Uses ``forPath`` for path-based locators and ``forName`` for Unity Catalog.

    Args:
        spark:   Active SparkSession.
        spec:    Target spec carrying the table reference.
        locator: Path-based locator, or ``None`` for Unity Catalog.

    Returns:
        A ``DeltaTable`` instance aliased to :data:`~loom.etl._upsert.TARGET_ALIAS`.
    """
    from delta.tables import DeltaTable

    if spec.table_ref is None:
        raise TypeError("table_ref must be set for Delta table resolution")
    if locator is not None:
        uri = locator.locate(spec.table_ref).uri
        return DeltaTable.forPath(spark, uri).alias(TARGET_ALIAS)
    return DeltaTable.forName(spark, spec.table_ref.ref).alias(TARGET_ALIAS)


def _merge_spark(
    spark: SparkSession,
    df: DataFrame,
    spec: TargetSpec,
    locator: TableLocator | None,
) -> None:
    """Execute a Delta MERGE (UPSERT) for the Spark backend.

    Args:
        spark:   Active SparkSession.
        df:      Source DataFrame (after schema validation).
        spec:    Target spec carrying keys, partition cols, exclude/include.
        locator: Path-based locator, or ``None`` for Unity Catalog.
    """
    if spec.table_ref is None:
        raise TypeError("table_ref must be set for MERGE operations")
    table_ref_str = spec.table_ref.ref
    combos = _collect_partition_combos_for_merge_spark(df, spec.partition_cols, table_ref_str)
    predicate = _build_upsert_predicate(combos, spec, TARGET_ALIAS, SOURCE_ALIAS)
    update_cols = _build_upsert_update_cols(tuple(df.columns), spec)
    update_set = _build_update_set(update_cols, SOURCE_ALIAS)

    dt = _resolve_delta_table_spark(spark, spec, locator)
    (
        dt.merge(df.alias(SOURCE_ALIAS), predicate)
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsertAll()
        .execute()
    )


def _sort_for_write(df: DataFrame, spec: TargetSpec) -> DataFrame:
    """Apply ``sortWithinPartitions`` for write modes that benefit from locality.

    Does not repartition (no full shuffle) — only sorts within existing
    Spark partitions so Delta can write contiguous blocks per partition key.
    Combined with ``optimizeWrite=true``, Delta handles file sizing.

    ``repartition`` is deliberately avoided: for a single partition value
    (the common daily-batch case) it would concentrate all rows on one
    worker, causing OOM or extreme slowness.
    """
    cols = spec.partition_cols or spec.upsert_keys
    if cols and spec.mode in (
        WriteMode.REPLACE_PARTITIONS,
        WriteMode.REPLACE_WHERE,
        WriteMode.UPSERT,
    ):
        return df.sortWithinPartitions(*cols)
    return df
