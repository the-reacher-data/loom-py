"""SparkDeltaWriter — TargetWriter backed by PySpark + Delta Lake."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from loom.etl._io import TableDiscovery
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
            self._write_frame(frame, spec)
            self._register_schema(table_ref, frame)
            return

        validated = spark_apply_schema(frame, existing_schema, spec.schema_mode)
        self._write_frame(validated, spec)
        self._register_schema(table_ref, validated)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _write_frame(self, df: DataFrame, spec: TargetSpec) -> None:
        path = self._table_path(spec.table_ref)  # type: ignore[arg-type]
        path.mkdir(parents=True, exist_ok=True)

        writer = df.write.format("delta")

        if spec.mode is WriteMode.APPEND:
            writer = writer.mode("append")
            if spec.schema_mode is SchemaMode.EVOLVE:
                writer = writer.option("mergeSchema", "true")
        else:
            writer = writer.mode("overwrite")
            if spec.schema_mode is SchemaMode.OVERWRITE:
                writer = writer.option("overwriteSchema", "true")

        writer.save(str(path))

    def _register_schema(self, ref: TableRef, frame: DataFrame) -> None:
        schema = tuple(
            ColumnSchema(name=f.name, dtype=spark_to_loom(f.dataType)) for f in frame.schema.fields
        )
        self._catalog.update_schema(ref, schema)

    def _table_path(self, ref: TableRef) -> Path:
        return self._root.joinpath(*ref.ref.split("."))
