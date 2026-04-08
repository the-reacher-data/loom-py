"""Spark write executor for planned table operations."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as t

from loom.etl.backends.spark._schema import spark_apply_schema
from loom.etl.backends.spark._writer import (
    _first_run_overwrite_partitions_spark,
    _first_run_overwrite_spark,
    _merge_spark,
    _sink,
    _write_replace_partitions_spark,
)
from loom.etl.io.target import SchemaMode
from loom.etl.schema._table import TableRef
from loom.etl.sql._predicate_sql import predicate_to_sql
from loom.etl.storage._locator import TableLocation, TableLocator
from loom.etl.storage.route import CatalogTarget, PathTarget
from loom.etl.storage.schema.model import PhysicalSchema, SparkPhysicalSchema
from loom.etl.storage.write import (
    AppendOp,
    ReplaceOp,
    ReplacePartitionsOp,
    ReplaceWhereOp,
    UpsertOp,
    WriteOperation,
)


class SparkWriteExecutor:
    """Execute write operations against Spark Delta targets."""

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark
        self._handlers: dict[type[WriteOperation], Callable[..., None]] = {
            AppendOp: self._exec_append,
            ReplaceOp: self._exec_replace,
            ReplacePartitionsOp: self._exec_replace_partitions,
            ReplaceWhereOp: self._exec_replace_where,
            UpsertOp: self._exec_upsert,
        }

    def execute(self, frame: DataFrame, op: WriteOperation, params_instance: Any) -> None:
        """Execute one write operation."""
        handler = self._handlers.get(type(op))
        if handler is None:
            raise TypeError(f"Unsupported write operation: {type(op)!r}")
        handler(frame, op, params_instance)

    def _exec_append(self, frame: DataFrame, op: AppendOp, params_instance: Any) -> None:
        locator = _locator_for_target(op.target)
        validated = _validated_frame(frame, op.existing_schema, op.schema_mode)
        _write_frame(validated, op, params_instance, locator)

    def _exec_replace(self, frame: DataFrame, op: ReplaceOp, params_instance: Any) -> None:
        locator = _locator_for_target(op.target)
        if op.existing_schema is None and op.schema_mode is SchemaMode.OVERWRITE:
            _write_frame(frame, op, params_instance, locator)
            return
        validated = _validated_frame(frame, op.existing_schema, op.schema_mode)
        _write_frame(validated, op, params_instance, locator)

    def _exec_replace_partitions(
        self, frame: DataFrame, op: ReplacePartitionsOp, _params_instance: Any
    ) -> None:
        locator = _locator_for_target(op.target)
        table_ref = op.target.logical_ref
        if op.existing_schema is None and op.schema_mode is SchemaMode.OVERWRITE:
            _first_run_overwrite_partitions_spark(frame, op.partition_cols, table_ref, locator)
            return
        validated = _validated_frame(frame, op.existing_schema, op.schema_mode)
        _write_replace_partitions_spark(validated, op.partition_cols, table_ref, locator)

    def _exec_replace_where(
        self, frame: DataFrame, op: ReplaceWhereOp, params_instance: Any
    ) -> None:
        locator = _locator_for_target(op.target)
        if op.existing_schema is None and op.schema_mode is SchemaMode.OVERWRITE:
            _write_frame(frame, op, params_instance, locator)
            return
        validated = _validated_frame(frame, op.existing_schema, op.schema_mode)
        _write_frame(validated, op, params_instance, locator)

    def _exec_upsert(self, frame: DataFrame, op: UpsertOp, _params_instance: Any) -> None:
        locator = _locator_for_target(op.target)
        table_ref = op.target.logical_ref
        if op.existing_schema is None:
            _first_run_overwrite_spark(frame, table_ref, locator)
            return
        validated = _validated_frame(frame, op.existing_schema, op.schema_mode)
        _merge_spark(self._spark, validated, op, table_ref, locator)


def _validated_frame(
    frame: DataFrame, existing_schema: PhysicalSchema | None, mode: SchemaMode
) -> DataFrame:
    return spark_apply_schema(frame, _as_struct_type(existing_schema), mode)


def _as_struct_type(schema: PhysicalSchema | None) -> t.StructType | None:
    if schema is None:
        return None
    if not isinstance(schema, SparkPhysicalSchema):
        raise TypeError(f"Spark executor expected SparkPhysicalSchema, got {type(schema)!r}.")
    return schema.schema


def _write_frame(
    frame: DataFrame,
    op: AppendOp | ReplaceOp | ReplaceWhereOp,
    params: Any,
    locator: TableLocator | None,
) -> None:
    table_ref = op.target.logical_ref
    writer = frame.write.format("delta").option("optimizeWrite", "true")
    match op:
        case AppendOp():
            writer = writer.mode("append")
            if op.schema_mode is SchemaMode.EVOLVE:
                writer = writer.option("mergeSchema", "true")
        case ReplaceWhereOp():
            predicate = predicate_to_sql(op.replace_predicate, params)
            writer = writer.mode("overwrite").option("replaceWhere", predicate)
        case ReplaceOp():
            writer = writer.mode("overwrite")
            if op.schema_mode is SchemaMode.OVERWRITE:
                writer = writer.option("overwriteSchema", "true")
    _sink(writer, table_ref, locator)


def _locator_for_target(target: CatalogTarget | PathTarget) -> TableLocator | None:
    if isinstance(target, CatalogTarget):
        return None
    return _SingleLocationLocator(target.logical_ref, target.location)


class _SingleLocationLocator:
    def __init__(self, expected_ref: TableRef, location: TableLocation) -> None:
        self._expected_ref = expected_ref
        self._location = location

    def locate(self, ref: TableRef) -> TableLocation:
        if ref != self._expected_ref:
            raise KeyError(
                f"Single-location locator cannot resolve {ref.ref!r}; "
                f"expected {self._expected_ref.ref!r}."
            )
        return self._location
