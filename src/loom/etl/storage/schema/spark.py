"""Spark-backed physical schema reader."""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession

from loom.etl.backends.spark._dtype import spark_to_loom
from loom.etl.schema._schema import ColumnSchema
from loom.etl.storage.route.model import CatalogTarget, ResolvedTarget
from loom.etl.storage.schema.model import PhysicalSchema


class SparkSchemaReader:
    """Read table schema/partition metadata through Spark + DeltaTable."""

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def read_schema(self, target: ResolvedTarget) -> PhysicalSchema | None:
        """Return physical schema, or ``None`` when table does not exist."""
        if isinstance(target, CatalogTarget):
            return self._read_catalog(target.catalog_ref.ref)
        return self._read_path(target.location.uri)

    def _read_catalog(self, ref: str) -> PhysicalSchema | None:
        if not self._spark.catalog.tableExists(ref):
            return None
        df = self._spark.table(ref)
        return PhysicalSchema(
            columns=_spark_columns(df.schema.fields),
            partition_columns=_partition_columns_for_name(self._spark, ref),
        )

    def _read_path(self, uri: str) -> PhysicalSchema | None:
        try:
            df = self._spark.read.format("delta").load(uri)
        except AnalysisException:
            return None
        return PhysicalSchema(
            columns=_spark_columns(df.schema.fields),
            partition_columns=_partition_columns_for_path(self._spark, uri),
        )


def _spark_columns(fields: Sequence[object]) -> tuple[ColumnSchema, ...]:
    from pyspark.sql import types as t

    typed = tuple(fields)
    return tuple(
        ColumnSchema(name=f.name, dtype=spark_to_loom(f.dataType), nullable=f.nullable)
        for f in typed
        if isinstance(f, t.StructField)
    )


def _partition_columns_for_name(spark: SparkSession, ref: str) -> tuple[str, ...]:
    from delta.tables import DeltaTable

    try:
        values = DeltaTable.forName(spark, ref).detail().select("partitionColumns").collect()
    except Exception:
        return ()
    if not values:
        return ()
    first = values[0][0]
    if isinstance(first, list):
        return tuple(str(v) for v in first)
    return ()


def _partition_columns_for_path(spark: SparkSession, uri: str) -> tuple[str, ...]:
    from delta.tables import DeltaTable

    try:
        values = DeltaTable.forPath(spark, uri).detail().select("partitionColumns").collect()
    except Exception:
        return ()
    if not values:
        return ()
    first = values[0][0]
    if isinstance(first, list):
        return tuple(str(v) for v in first)
    return ()
