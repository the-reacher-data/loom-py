"""Spark-backed execution record writer."""

from __future__ import annotations

from typing import Any

from loom.etl.observability.records import (
    ExecutionRecord,
    PipelineRunRecord,
    ProcessRunRecord,
    StepRunRecord,
)
from loom.etl.observability.writers._row import record_to_row
from loom.etl.schema._table import TableRef


class SparkExecutionRecordWriter:
    """Write execution records through the Spark target writer ``append`` API."""

    def __init__(self, spark: Any, writer: Any) -> None:
        self._spark = spark
        self._writer = writer

    def write_record(self, record: ExecutionRecord, table_ref: TableRef, /) -> None:
        """Append one execution record row to *table_ref*."""
        frame = self._spark.createDataFrame([record_to_row(record)], schema=_spark_schema(record))
        self._writer.append(frame, table_ref, None)


def _spark_schema(record: ExecutionRecord) -> Any:
    from pyspark.sql import types as t

    if isinstance(record, PipelineRunRecord):
        return t.StructType(
            [
                t.StructField("event", t.StringType(), False),
                t.StructField("run_id", t.StringType(), False),
                t.StructField("correlation_id", t.StringType(), True),
                t.StructField("attempt", t.LongType(), False),
                t.StructField("pipeline", t.StringType(), False),
                t.StructField("started_at", t.TimestampType(), False),
                t.StructField("status", t.StringType(), False),
                t.StructField("duration_ms", t.LongType(), False),
                t.StructField("error", t.StringType(), True),
            ]
        )
    if isinstance(record, ProcessRunRecord):
        return t.StructType(
            [
                t.StructField("event", t.StringType(), False),
                t.StructField("run_id", t.StringType(), False),
                t.StructField("correlation_id", t.StringType(), True),
                t.StructField("attempt", t.LongType(), False),
                t.StructField("process_run_id", t.StringType(), False),
                t.StructField("process", t.StringType(), False),
                t.StructField("started_at", t.TimestampType(), False),
                t.StructField("status", t.StringType(), False),
                t.StructField("duration_ms", t.LongType(), False),
                t.StructField("error", t.StringType(), True),
            ]
        )
    if isinstance(record, StepRunRecord):
        return t.StructType(
            [
                t.StructField("event", t.StringType(), False),
                t.StructField("run_id", t.StringType(), False),
                t.StructField("correlation_id", t.StringType(), True),
                t.StructField("attempt", t.LongType(), False),
                t.StructField("step_run_id", t.StringType(), False),
                t.StructField("step", t.StringType(), False),
                t.StructField("started_at", t.TimestampType(), False),
                t.StructField("status", t.StringType(), False),
                t.StructField("duration_ms", t.LongType(), False),
                t.StructField("error", t.StringType(), True),
            ]
        )
    raise TypeError(f"Unsupported execution record type: {type(record)!r}")


__all__ = ["SparkExecutionRecordWriter"]
