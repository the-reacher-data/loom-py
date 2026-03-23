"""SparkDeltaWriter — TargetWriter backed by PySpark + Delta Lake."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from loom.etl._io import TableDiscovery
from loom.etl._predicate_sql import predicate_to_sql
from loom.etl._schema import ColumnSchema
from loom.etl._table import TableRef
from loom.etl._target import SchemaMode, TargetSpec, WriteMode
from loom.etl.backends.spark._dtype import spark_to_loom
from loom.etl.backends.spark._schema import spark_apply_schema


class SparkDeltaWriter:
    """Write ETL step results to Delta tables using PySpark + Delta Lake.

    Implements :class:`~loom.etl._io.TargetWriter`.

    Validates or evolves the frame schema via
    :func:`~loom.etl.backends.spark._schema.spark_apply_schema` before each
    write, then updates the catalog so later steps see the evolved schema.

    OVERWRITE mode is the only mode allowed to create a new table from scratch.

    Args:
        spark:   Active :class:`pyspark.sql.SparkSession`.
        root:    Filesystem root.
        catalog: Catalog used for schema lookup and post-write update.

    Example::

        writer = SparkDeltaWriter(spark, Path("/data/delta"), catalog)
        writer.write(frame, spec, params)
    """

    def __init__(self, spark: SparkSession, root: Path, catalog: TableDiscovery) -> None:
        self._spark = spark
        self._root = root
        self._catalog = catalog

    def write(self, frame: DataFrame, spec: TargetSpec, params_instance: Any) -> None:
        """Validate schema and write *frame* to the Delta target.

        Args:
            frame:           Spark DataFrame produced by the step's ``execute()``.
            spec:            Compiled target spec.
            params_instance: Concrete params (unused currently).

        Raises:
            AssertionError:      If *spec* is a FILE target.
            SchemaNotFoundError: When the table has no registered schema and
                                 mode is not OVERWRITE.
            SchemaError:         When the frame violates the registered schema.
        """
        assert spec.table_ref is not None, (
            f"SparkDeltaWriter only supports TABLE targets; got FILE spec: {spec}"
        )

        table_ref = spec.table_ref
        existing_schema = self._catalog.schema(table_ref)

        if existing_schema is None and spec.schema_mode is SchemaMode.OVERWRITE:
            self._write_frame(frame, spec, params_instance)
            self._register_schema(table_ref, frame)
            return

        validated = spark_apply_schema(frame, existing_schema, spec.schema_mode)
        self._write_frame(validated, spec, params_instance)
        self._register_schema(table_ref, validated)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _write_frame(self, df: DataFrame, spec: TargetSpec, params_instance: Any) -> None:
        path = self._table_path(spec.table_ref)  # type: ignore[arg-type]
        path.mkdir(parents=True, exist_ok=True)

        df = _sort_for_write(df, spec)
        writer = df.write.format("delta").option("optimizeWrite", "true")
        writer = _MODE_APPLIERS[spec.mode](writer, df, spec, params_instance)
        writer.save(str(path))

    def _register_schema(self, ref: TableRef, frame: DataFrame) -> None:
        schema = tuple(
            ColumnSchema(name=f.name, dtype=spark_to_loom(f.dataType)) for f in frame.schema.fields
        )
        self._catalog.update_schema(ref, schema)

    def _table_path(self, ref: TableRef) -> Path:
        return self._root.joinpath(*ref.ref.split("."))


# ------------------------------------------------------------------
# Write mode appliers — one function per mode, composed via dispatch map
# ------------------------------------------------------------------


def _apply_append(writer: Any, _df: DataFrame, spec: TargetSpec, _params: Any) -> Any:
    writer = writer.mode("append")
    if spec.schema_mode is SchemaMode.EVOLVE:
        writer = writer.option("mergeSchema", "true")
    return writer


def _apply_replace_partitions(writer: Any, df: DataFrame, spec: TargetSpec, _params: Any) -> Any:
    predicate = _build_partition_predicate(df, spec.partition_cols)
    return writer.mode("overwrite").option("replaceWhere", predicate)


def _apply_replace_where(writer: Any, _df: DataFrame, spec: TargetSpec, params: Any) -> Any:
    predicate = predicate_to_sql(spec.replace_predicate, params)  # type: ignore[arg-type]
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
    WriteMode.UPSERT: _apply_overwrite,
}


# ------------------------------------------------------------------
# Shared helpers
# ------------------------------------------------------------------


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


def _build_partition_predicate(df: DataFrame, partition_cols: tuple[str, ...]) -> str:
    """Build a ``replaceWhere`` SQL predicate from the distinct partition values in *df*.

    Collects only the partition columns — typically O(1-31) distinct rows.
    """
    rows = df.select(*partition_cols).distinct().collect()
    clauses = [
        " AND ".join(f"{col} = {_sql_literal(row[col])}" for col in partition_cols) for row in rows
    ]
    return " OR ".join(f"({c})" for c in clauses)


def _sql_literal(value: Any) -> str:
    if isinstance(value, str):
        return f"'{value.replace(chr(39), chr(39) * 2)}'"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return str(value)
