"""Spark write executor for planned table operations."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as t

from loom.etl.backends.spark._schema import spark_apply_schema
from loom.etl.backends.spark._writer import (
    _apply_append,
    _apply_overwrite,
    _apply_replace_where,
    _first_run_overwrite_partitions_spark,
    _first_run_overwrite_spark,
    _merge_spark,
    _sink,
    _write_replace_partitions_spark,
)
from loom.etl.io.target import SchemaMode
from loom.etl.io.target._table import (
    AppendSpec,
    ReplacePartitionsSpec,
    ReplaceSpec,
    ReplaceWhereSpec,
    UpsertSpec,
)
from loom.etl.schema._table import TableRef
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
        self._handlers = {
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

    def _exec_append(self, frame: DataFrame, op: WriteOperation, params_instance: Any) -> None:
        if not isinstance(op, AppendOp):
            raise TypeError(f"Expected AppendOp, got {type(op)!r}")
        locator = _locator_for_target(op.target)
        spec = AppendSpec(table_ref=op.target.logical_ref, schema_mode=op.schema_mode)
        validated = _validated_frame(frame, op.existing_schema, op.schema_mode)
        _write_frame(validated, spec, params_instance, locator)

    def _exec_replace(self, frame: DataFrame, op: WriteOperation, params_instance: Any) -> None:
        if not isinstance(op, ReplaceOp):
            raise TypeError(f"Expected ReplaceOp, got {type(op)!r}")
        locator = _locator_for_target(op.target)
        spec = ReplaceSpec(table_ref=op.target.logical_ref, schema_mode=op.schema_mode)
        if op.existing_schema is None and op.schema_mode is SchemaMode.OVERWRITE:
            _write_frame(frame, spec, params_instance, locator)
            return
        validated = _validated_frame(frame, op.existing_schema, op.schema_mode)
        _write_frame(validated, spec, params_instance, locator)

    def _exec_replace_partitions(
        self, frame: DataFrame, op: WriteOperation, _params_instance: Any
    ) -> None:
        if not isinstance(op, ReplacePartitionsOp):
            raise TypeError(f"Expected ReplacePartitionsOp, got {type(op)!r}")
        locator = _locator_for_target(op.target)
        spec = ReplacePartitionsSpec(
            table_ref=op.target.logical_ref,
            partition_cols=op.partition_cols,
            schema_mode=op.schema_mode,
        )
        if op.existing_schema is None and op.schema_mode is SchemaMode.OVERWRITE:
            _first_run_overwrite_partitions_spark(frame, spec, locator)
            return
        validated = _validated_frame(frame, op.existing_schema, op.schema_mode)
        _write_replace_partitions_spark(validated, spec, locator)

    def _exec_replace_where(
        self, frame: DataFrame, op: WriteOperation, params_instance: Any
    ) -> None:
        if not isinstance(op, ReplaceWhereOp):
            raise TypeError(f"Expected ReplaceWhereOp, got {type(op)!r}")
        locator = _locator_for_target(op.target)
        spec = ReplaceWhereSpec(
            table_ref=op.target.logical_ref,
            replace_predicate=op.replace_predicate,
            schema_mode=op.schema_mode,
        )
        if op.existing_schema is None and op.schema_mode is SchemaMode.OVERWRITE:
            _write_frame(frame, spec, params_instance, locator)
            return
        validated = _validated_frame(frame, op.existing_schema, op.schema_mode)
        _write_frame(validated, spec, params_instance, locator)

    def _exec_upsert(self, frame: DataFrame, op: WriteOperation, _params_instance: Any) -> None:
        if not isinstance(op, UpsertOp):
            raise TypeError(f"Expected UpsertOp, got {type(op)!r}")
        locator = _locator_for_target(op.target)
        spec = UpsertSpec(
            table_ref=op.target.logical_ref,
            upsert_keys=op.upsert_keys,
            partition_cols=op.partition_cols,
            upsert_exclude=op.upsert_exclude,
            upsert_include=op.upsert_include,
            schema_mode=op.schema_mode,
        )
        if op.existing_schema is None:
            _first_run_overwrite_spark(frame, spec, locator)
            return
        validated = _validated_frame(frame, op.existing_schema, op.schema_mode)
        _merge_spark(self._spark, validated, spec, locator)


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
    spec: AppendSpec | ReplaceSpec | ReplaceWhereSpec,
    params: Any,
    locator: TableLocator | None,
) -> None:
    writer = frame.write.format("delta").option("optimizeWrite", "true")
    if isinstance(spec, AppendSpec):
        writer = _apply_append(writer, spec)
    elif isinstance(spec, ReplaceWhereSpec):
        writer = _apply_replace_where(writer, spec, params)
    else:
        writer = _apply_overwrite(writer, spec)
    _sink(writer, spec.table_ref, locator)


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
