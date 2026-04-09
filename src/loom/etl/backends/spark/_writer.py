"""Spark target writer implementing _WritePolicy hooks."""

from __future__ import annotations

import logging
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from loom.etl.backends._predicate import predicate_to_sql
from loom.etl.backends._upsert import (
    _log_partition_combos,
    _warn_no_partition_cols,
)
from loom.etl.backends._write_policy import _WritePolicy
from loom.etl.io.target import SchemaMode
from loom.etl.io.target._file import FileSpec
from loom.etl.io.target._table import UpsertSpec
from loom.etl.storage._config import MissingTablePolicy
from loom.etl.storage.routing import (
    CatalogRouteResolver,
    CatalogTarget,
    PathRouteResolver,
    ResolvedTarget,
    TableRouteResolver,
)
from loom.etl.storage.schema import SparkPhysicalSchema

_log = logging.getLogger(__name__)


class SparkTargetWriter(_WritePolicy[DataFrame, SparkPhysicalSchema]):
    """Spark target writer using Delta Lake."""

    def __init__(
        self,
        spark: SparkSession,
        locator: str | None = None,
        *,
        route_resolver: TableRouteResolver | None = None,
        missing_table_policy: MissingTablePolicy = MissingTablePolicy.SCHEMA_MODE,
    ) -> None:
        self._spark = spark

        if route_resolver is None:
            if locator is None:
                resolver: TableRouteResolver = CatalogRouteResolver()
            else:
                from loom.etl.storage._locator import _as_locator

                resolver = PathRouteResolver(_as_locator(locator))
        else:
            resolver = route_resolver

        super().__init__(
            resolver=resolver,
            missing_table_policy=missing_table_policy,
        )

    def append(
        self,
        frame: DataFrame,
        table_ref: Any,
        params_instance: Any,
        *,
        streaming: bool = False,
    ) -> None:
        """Append frame to table (legacy API, creates table on first write)."""
        from loom.etl.io.target._table import AppendSpec

        spec = AppendSpec(table_ref=table_ref, schema_mode=SchemaMode.EVOLVE)
        self.write(frame, spec, params_instance, streaming=streaming)

    # ====================================================================
    # Schema Hooks
    # ====================================================================

    def _physical_schema(self, target: ResolvedTarget) -> SparkPhysicalSchema | None:
        """Read physical schema from Spark/Delta."""
        from pyspark.sql import types as T

        if isinstance(target, CatalogTarget):
            if not self._spark.catalog.tableExists(target.catalog_ref.ref):
                return None
            fields = self._spark.table(target.catalog_ref.ref).schema.fields
            return SparkPhysicalSchema(schema=T.StructType(list(fields)))

        # Path target
        try:
            dt = DeltaTable.forPath(self._spark, target.location.uri)
            return SparkPhysicalSchema(schema=dt.toDF().schema)
        except Exception:
            return None

    def _align(
        self,
        frame: DataFrame,
        existing_schema: SparkPhysicalSchema | None,
        mode: SchemaMode,
    ) -> DataFrame:
        """Align frame schema with existing."""
        if existing_schema is None or mode is SchemaMode.OVERWRITE:
            return frame

        from loom.etl.backends.spark._schema import spark_apply_schema

        return spark_apply_schema(frame, existing_schema.schema, mode)

    def _materialize(self, frame: DataFrame, streaming: bool) -> DataFrame:
        """Spark DataFrames are already eager."""
        _ = streaming
        return frame

    def _predicate_to_sql(self, predicate: Any, params: Any) -> str:
        """Convert predicate to SQL."""
        return predicate_to_sql(predicate, params)

    # ====================================================================
    # Write Hooks
    # ====================================================================

    def _create(
        self,
        frame: DataFrame,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
        partition_cols: tuple[str, ...] = (),
    ) -> None:
        """Create new Delta table."""
        writer = (
            frame.write.format("delta")
            .option("optimizeWrite", "true")
            .mode("overwrite")
            .option("overwriteSchema", "true")
        )
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        self._sink(writer, target)

    def _append(
        self,
        frame: DataFrame,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Append to existing Delta table."""
        writer = frame.write.format("delta").option("optimizeWrite", "true").mode("append")
        if schema_mode is SchemaMode.EVOLVE:
            writer = writer.option("mergeSchema", "true")
        self._sink(writer, target)

    def _replace(
        self,
        frame: DataFrame,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Overwrite existing Delta table."""
        writer = frame.write.format("delta").option("optimizeWrite", "true").mode("overwrite")
        if schema_mode is SchemaMode.OVERWRITE:
            writer = writer.option("overwriteSchema", "true")
        self._sink(writer, target)

    def _replace_partitions(
        self,
        frame: DataFrame,
        target: ResolvedTarget,
        *,
        partition_cols: tuple[str, ...],
        schema_mode: SchemaMode,
    ) -> None:
        """Overwrite partitions present in frame."""
        rows = frame.select(*partition_cols).distinct().collect()
        if not rows:
            return

        predicates = [
            f"({' AND '.join(f'{c} = {repr(row[c])}' for c in partition_cols)})" for row in rows
        ]
        predicate = " OR ".join(predicates)

        writer = (
            frame.sortWithinPartitions(*partition_cols)
            .write.format("delta")
            .option("optimizeWrite", "true")
            .mode("overwrite")
            .option("replaceWhere", predicate)
        )
        self._sink(writer, target)

    def _replace_where(
        self,
        frame: DataFrame,
        target: ResolvedTarget,
        *,
        predicate: str,
        schema_mode: SchemaMode,
    ) -> None:
        """Overwrite rows matching SQL predicate."""
        writer = (
            frame.write.format("delta")
            .option("optimizeWrite", "true")
            .mode("overwrite")
            .option("replaceWhere", predicate)
        )
        self._sink(writer, target)

    def _upsert(
        self,
        frame: DataFrame,
        target: ResolvedTarget,
        *,
        spec: UpsertSpec,
        existing_schema: SparkPhysicalSchema,
    ) -> None:
        """Merge frame into target using Delta MERGE (delta-spark)."""
        _ = existing_schema

        # Get DeltaTable using delta-spark library
        if isinstance(target, CatalogTarget):
            dt = DeltaTable.forName(self._spark, target.catalog_ref.ref)
        else:
            dt = DeltaTable.forPath(self._spark, target.location.uri)

        # Build merge predicate: join on upsert keys + partition cols
        all_keys = list(spec.upsert_keys) + list(spec.partition_cols)
        merge_pred = " AND ".join(f"target.{k} = source.{k}" for k in all_keys)

        # Determine columns to update (exclude upsert keys by default)
        if spec.upsert_include:
            update_cols = list(spec.upsert_include)
        elif spec.upsert_exclude:
            update_cols = [c for c in frame.columns if c not in spec.upsert_exclude]
        else:
            update_cols = [c for c in frame.columns if c not in spec.upsert_keys]

        # Execute MERGE
        (
            dt.alias("target")
            .merge(frame.alias("source"), merge_pred)
            .whenMatchedUpdate(set={c: F.col(f"source.{c}") for c in update_cols})
            .whenNotMatchedInsert(values={c: F.col(f"source.{c}") for c in frame.columns})
            .execute()
        )

    def _write_file(
        self,
        frame: DataFrame,
        spec: FileSpec,
        *,
        streaming: bool,
    ) -> None:
        """Write to file (CSV, JSON, Parquet)."""
        _ = streaming
        from loom.etl.io._format import Format

        fmt = spec.format.value if isinstance(spec.format, Format) else spec.format

        if fmt == "delta":
            frame.write.format("delta").mode("overwrite").save(spec.path)
            return

        if fmt == "csv":
            from loom.etl.io._write_options import CsvWriteOptions

            opts = (
                spec.write_options
                if isinstance(spec.write_options, CsvWriteOptions)
                else CsvWriteOptions()
            )
            writer = (
                frame.write.mode("overwrite")
                .option("sep", opts.separator)
                .option("header", str(opts.has_header).lower())
            )
            for key, value in opts.kwargs:
                writer = writer.option(key, str(value))
            writer.csv(spec.path)
            return

        if fmt == "json":
            from loom.etl.io._write_options import JsonWriteOptions

            opts = (
                spec.write_options
                if isinstance(spec.write_options, JsonWriteOptions)
                else JsonWriteOptions()
            )
            writer = frame.write.mode("overwrite")
            for key, value in opts.kwargs:
                writer = writer.option(key, str(value))
            writer.json(spec.path)
            return

        if fmt == "parquet":
            from loom.etl.io._write_options import ParquetWriteOptions

            opts = (
                spec.write_options
                if isinstance(spec.write_options, ParquetWriteOptions)
                else ParquetWriteOptions()
            )
            writer = frame.write.mode("overwrite").option("compression", opts.compression)
            for key, value in opts.kwargs:
                writer = writer.option(key, str(value))
            writer.parquet(spec.path)
            return

        raise ValueError(f"Unsupported format: {fmt}")

    # ====================================================================
    # Helpers
    # ====================================================================

    def _sink(self, writer: Any, target: ResolvedTarget) -> None:
        """Finalize write based on target type."""
        if isinstance(target, CatalogTarget):
            writer.saveAsTable(target.catalog_ref.ref)
        else:
            writer.save(target.location.uri)

    def _collect_partition_combos(
        self,
        frame: DataFrame,
        partition_cols: tuple[str, ...],
        table_ref: str,
    ) -> list[dict[str, Any]]:
        """Collect unique partition value combinations."""
        if not partition_cols:
            _warn_no_partition_cols(table_ref)
            return []
        combos = frame.select(*partition_cols).distinct().collect()
        result = [{c: row[c] for c in partition_cols} for row in combos]
        _log_partition_combos(result, table_ref)
        return result

    class _MergeSpecAdapter:
        """Adapter to make UpsertSpec compatible with shared _upsert.py helpers."""

        def __init__(
            self,
            upsert_keys: tuple[str, ...],
            partition_cols: tuple[str, ...],
            upsert_exclude: tuple[str, ...],
            upsert_include: tuple[str, ...],
        ) -> None:
            self.upsert_keys = upsert_keys
            self.partition_cols = partition_cols
            self.upsert_exclude = upsert_exclude
            self.upsert_include = upsert_include


__all__ = ["SparkTargetWriter"]
