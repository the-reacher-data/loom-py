"""Spark-backed execution record writer."""

from __future__ import annotations

import dataclasses
from typing import Any

from pyspark.sql import types as _spark_types

from loom.etl.declarative.expr._refs import TableRef
from loom.etl.observability.records import (
    ExecutionRecord,
    PipelineRunRecord,
    ProcessRunRecord,
    StepRunRecord,
)


def _record_to_row(record: ExecutionRecord) -> dict[str, Any]:
    """Convert an execution record dataclass into a plain row mapping."""
    row = dataclasses.asdict(record)
    # Persist only snapshot fields in Delta tables; lifecycle event type is
    # still used by log observers but does not add analytical value here.
    row.pop("event", None)
    row["status"] = str(row["status"])
    return row


def _spark_schema(record: ExecutionRecord) -> Any:
    t = _spark_types

    if isinstance(record, PipelineRunRecord):
        return t.StructType(
            [
                t.StructField("run_id", t.StringType(), False),
                t.StructField("correlation_id", t.StringType(), True),
                t.StructField("attempt", t.LongType(), False),
                t.StructField("pipeline", t.StringType(), False),
                t.StructField("started_at", t.TimestampType(), False),
                t.StructField("status", t.StringType(), False),
                t.StructField("duration_ms", t.LongType(), False),
                t.StructField("error", t.StringType(), True),
                t.StructField("error_type", t.StringType(), True),
                t.StructField("error_message", t.StringType(), True),
                t.StructField("failed_step_run_id", t.StringType(), True),
                t.StructField("failed_step", t.StringType(), True),
            ]
        )
    if isinstance(record, ProcessRunRecord):
        return t.StructType(
            [
                t.StructField("run_id", t.StringType(), False),
                t.StructField("correlation_id", t.StringType(), True),
                t.StructField("attempt", t.LongType(), False),
                t.StructField("process_run_id", t.StringType(), False),
                t.StructField("process", t.StringType(), False),
                t.StructField("started_at", t.TimestampType(), False),
                t.StructField("status", t.StringType(), False),
                t.StructField("duration_ms", t.LongType(), False),
                t.StructField("error", t.StringType(), True),
                t.StructField("error_type", t.StringType(), True),
                t.StructField("error_message", t.StringType(), True),
                t.StructField("failed_step_run_id", t.StringType(), True),
                t.StructField("failed_step", t.StringType(), True),
            ]
        )
    if isinstance(record, StepRunRecord):
        return t.StructType(
            [
                t.StructField("run_id", t.StringType(), False),
                t.StructField("correlation_id", t.StringType(), True),
                t.StructField("attempt", t.LongType(), False),
                t.StructField("step_run_id", t.StringType(), False),
                t.StructField("step", t.StringType(), False),
                t.StructField("started_at", t.TimestampType(), False),
                t.StructField("status", t.StringType(), False),
                t.StructField("duration_ms", t.LongType(), False),
                t.StructField("error", t.StringType(), True),
                t.StructField("process_run_id", t.StringType(), True),
                t.StructField("error_type", t.StringType(), True),
                t.StructField("error_message", t.StringType(), True),
            ]
        )
    raise TypeError(f"Unsupported execution record type: {type(record)!r}")


class SparkExecutionRecordWriter:
    """Write execution records through the Spark target writer ``append`` API."""

    def __init__(self, spark: Any, writer: Any) -> None:
        self._spark = spark
        self._writer = writer

    def write_record(self, record: ExecutionRecord, table_ref: TableRef, /) -> None:
        """Append one execution record row to *table_ref*."""
        frame = self._spark.createDataFrame([_record_to_row(record)], schema=_spark_schema(record))
        self._writer.append(frame, table_ref, None)


__all__ = ["SparkExecutionRecordWriter"]
