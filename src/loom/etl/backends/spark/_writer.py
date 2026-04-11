"""Spark target writer implementing _WritePolicy hooks."""

from __future__ import annotations

import dataclasses
import logging
from collections.abc import Callable, Sequence
from typing import Any, cast

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T
from pyspark.sql.column import Column

from loom.etl.backends._format_registry import resolve_format_handler
from loom.etl.backends._merge import (
    SOURCE_ALIAS,
    TARGET_ALIAS,
    _build_merge_plan,
    _log_partition_combos,
    _warn_no_partition_cols,
)
from loom.etl.backends._predicate import predicate_to_sql
from loom.etl.backends._write_policy import _WritePolicy
from loom.etl.backends.spark._schema import SparkPhysicalSchema, apply_schema_spark
from loom.etl.declarative._format import Format
from loom.etl.declarative._write_options import (
    CsvWriteOptions,
    JsonWriteOptions,
    ParquetWriteOptions,
)
from loom.etl.declarative.target import SchemaMode
from loom.etl.declarative.target._file import FileSpec
from loom.etl.declarative.target._table import AppendSpec, UpsertSpec
from loom.etl.observability.records import (
    ExecutionRecord,
    PipelineRunRecord,
    ProcessRunRecord,
    RunStatus,
    StepRunRecord,
)
from loom.etl.storage._config import MissingTablePolicy
from loom.etl.storage._locator import TableLocator, _as_locator
from loom.etl.storage.routing import (
    CatalogRouteResolver,
    CatalogTarget,
    PathRouteResolver,
    ResolvedTarget,
    TableRouteResolver,
)

_log = logging.getLogger(__name__)


class SparkTargetWriter(_WritePolicy[DataFrame, DataFrame, SparkPhysicalSchema]):
    """Spark target writer using Delta Lake."""

    def __init__(
        self,
        spark: SparkSession,
        locator: str | TableLocator | None = None,
        *,
        route_resolver: TableRouteResolver | None = None,
        missing_table_policy: MissingTablePolicy = MissingTablePolicy.SCHEMA_MODE,
    ) -> None:
        self._spark = spark

        if route_resolver is None:
            if locator is None:
                resolver: TableRouteResolver = CatalogRouteResolver()
            else:
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
        spec = AppendSpec(table_ref=table_ref, schema_mode=SchemaMode.EVOLVE)
        self.write(frame, spec, params_instance, streaming=streaming)

    def to_frame(self, records: Sequence[ExecutionRecord], /) -> DataFrame:
        """Convert execution records into a Spark DataFrame."""
        if not records:
            raise ValueError("SparkTargetWriter.to_frame requires at least one record.")
        first = records[0]
        record_type = type(first)
        if any(type(record) is not record_type for record in records):
            raise TypeError(
                "SparkTargetWriter.to_frame requires homogeneous record types per batch."
            )
        rows = [_record_to_row(record) for record in records]
        return self._spark.createDataFrame(rows, schema=_spark_record_schema(first))

    # ====================================================================
    # Schema Hooks
    # ====================================================================

    def _physical_schema(self, target: ResolvedTarget) -> SparkPhysicalSchema | None:
        """Read physical schema from Spark/Delta."""
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

        return apply_schema_spark(frame, existing_schema.schema, mode)

    def _materialize_for_write(self, frame: DataFrame, streaming: bool) -> DataFrame:
        """Return Spark frame unchanged (already materialized/eager)."""
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

        if isinstance(target, CatalogTarget):
            dt = DeltaTable.forName(self._spark, target.catalog_ref.ref)
        else:
            dt = DeltaTable.forPath(self._spark, target.location.uri)

        combos = self._collect_partition_combos(frame, spec.partition_cols, target.logical_ref.ref)
        merge_plan = _build_merge_plan(
            combos=combos,
            spec=spec,
            df_columns=tuple(frame.columns),
            target_alias=TARGET_ALIAS,
            source_alias=SOURCE_ALIAS,
        )

        (
            dt.alias(TARGET_ALIAS)
            .merge(frame.alias(SOURCE_ALIAS), merge_plan.predicate)
            .whenMatchedUpdate(set=cast(dict[str, str | Column], merge_plan.update_set))
            .whenNotMatchedInsert(values=cast(dict[str, str | Column], merge_plan.insert_values))
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
        writers: dict[Format, Callable[[DataFrame, FileSpec], None]] = {
            Format.DELTA: self._write_delta_file,
            Format.CSV: self._write_csv_file,
            Format.JSON: self._write_json_file,
            Format.PARQUET: self._write_parquet_file,
            Format.XLSX: self._write_xlsx_file,
        }
        writer = resolve_format_handler(spec.format, writers)
        writer(frame, spec)

    def _write_delta_file(self, frame: DataFrame, spec: FileSpec) -> None:
        frame.write.format("delta").mode("overwrite").save(spec.path)

    def _write_csv_file(self, frame: DataFrame, spec: FileSpec) -> None:
        csv_opts = (
            spec.write_options
            if isinstance(spec.write_options, CsvWriteOptions)
            else CsvWriteOptions()
        )
        writer = (
            frame.write.mode("overwrite")
            .option("sep", csv_opts.separator)
            .option("header", str(csv_opts.has_header).lower())
        )
        for key, value in csv_opts.kwargs:
            writer = writer.option(key, str(value))
        writer.csv(spec.path)

    def _write_json_file(self, frame: DataFrame, spec: FileSpec) -> None:
        json_opts = (
            spec.write_options
            if isinstance(spec.write_options, JsonWriteOptions)
            else JsonWriteOptions()
        )
        writer = frame.write.mode("overwrite")
        for key, value in json_opts.kwargs:
            writer = writer.option(key, str(value))
        writer.json(spec.path)

    def _write_parquet_file(self, frame: DataFrame, spec: FileSpec) -> None:
        parquet_opts = (
            spec.write_options
            if isinstance(spec.write_options, ParquetWriteOptions)
            else ParquetWriteOptions()
        )
        writer = frame.write.mode("overwrite").option("compression", parquet_opts.compression)
        for key, value in parquet_opts.kwargs:
            writer = writer.option(key, str(value))
        writer.parquet(spec.path)

    @staticmethod
    def _write_xlsx_file(_frame: DataFrame, _spec: FileSpec) -> None:
        raise TypeError("Spark backend does not support XLSX format.")

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


__all__ = ["SparkTargetWriter"]


def _record_to_row(record: ExecutionRecord) -> dict[str, Any]:
    """Convert an execution record dataclass into a plain row mapping."""
    row = dataclasses.asdict(record)
    row.pop("event", None)
    row["status"] = str(cast(RunStatus, row["status"]))
    return row


def _spark_record_schema(record: ExecutionRecord) -> T.StructType:
    if isinstance(record, PipelineRunRecord):
        return T.StructType(
            [
                T.StructField("run_id", T.StringType(), False),
                T.StructField("correlation_id", T.StringType(), True),
                T.StructField("attempt", T.LongType(), False),
                T.StructField("pipeline", T.StringType(), False),
                T.StructField("started_at", T.TimestampType(), False),
                T.StructField("status", T.StringType(), False),
                T.StructField("duration_ms", T.LongType(), False),
                T.StructField("error", T.StringType(), True),
                T.StructField("error_type", T.StringType(), True),
                T.StructField("error_message", T.StringType(), True),
                T.StructField("failed_step_run_id", T.StringType(), True),
                T.StructField("failed_step", T.StringType(), True),
            ]
        )
    if isinstance(record, ProcessRunRecord):
        return T.StructType(
            [
                T.StructField("run_id", T.StringType(), False),
                T.StructField("correlation_id", T.StringType(), True),
                T.StructField("attempt", T.LongType(), False),
                T.StructField("process_run_id", T.StringType(), False),
                T.StructField("process", T.StringType(), False),
                T.StructField("started_at", T.TimestampType(), False),
                T.StructField("status", T.StringType(), False),
                T.StructField("duration_ms", T.LongType(), False),
                T.StructField("error", T.StringType(), True),
                T.StructField("error_type", T.StringType(), True),
                T.StructField("error_message", T.StringType(), True),
                T.StructField("failed_step_run_id", T.StringType(), True),
                T.StructField("failed_step", T.StringType(), True),
            ]
        )
    if isinstance(record, StepRunRecord):
        return T.StructType(
            [
                T.StructField("run_id", T.StringType(), False),
                T.StructField("correlation_id", T.StringType(), True),
                T.StructField("attempt", T.LongType(), False),
                T.StructField("step_run_id", T.StringType(), False),
                T.StructField("step", T.StringType(), False),
                T.StructField("started_at", T.TimestampType(), False),
                T.StructField("status", T.StringType(), False),
                T.StructField("duration_ms", T.LongType(), False),
                T.StructField("error", T.StringType(), True),
                T.StructField("process_run_id", T.StringType(), True),
                T.StructField("error_type", T.StringType(), True),
                T.StructField("error_message", T.StringType(), True),
            ]
        )
    raise TypeError(f"Unsupported execution record type: {type(record)!r}")
