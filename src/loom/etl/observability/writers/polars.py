"""Polars-backed execution record writer."""

from __future__ import annotations

from typing import Any

import polars as pl

from loom.etl.observability.records import (
    ExecutionRecord,
    PipelineRunRecord,
    ProcessRunRecord,
    StepRunRecord,
)
from loom.etl.observability.writers._row import record_to_row
from loom.etl.schema._table import TableRef

# ---------------------------------------------------------------------------
# Explicit per-record-type Polars schemas
# ---------------------------------------------------------------------------
# These schemas are required to avoid Polars inferring `Null` dtype for
# optional fields (correlation_id, error) when they are None on first write.
# The order must match dataclasses.asdict() field order for each record type.

_PIPELINE_SCHEMA = pl.Schema(
    {
        "run_id": pl.String,
        "correlation_id": pl.String,
        "attempt": pl.Int64,
        "pipeline": pl.String,
        "started_at": pl.Datetime("us", "UTC"),
        "status": pl.String,
        "duration_ms": pl.Int64,
        "error": pl.String,
        "error_type": pl.String,
        "error_message": pl.String,
        "failed_step_run_id": pl.String,
        "failed_step": pl.String,
    }  # type: ignore[arg-type]  # mixed DataType class vs instance — valid at runtime
)

_PROCESS_SCHEMA = pl.Schema(
    {
        "run_id": pl.String,
        "correlation_id": pl.String,
        "attempt": pl.Int64,
        "process_run_id": pl.String,
        "process": pl.String,
        "started_at": pl.Datetime("us", "UTC"),
        "status": pl.String,
        "duration_ms": pl.Int64,
        "error": pl.String,
        "error_type": pl.String,
        "error_message": pl.String,
        "failed_step_run_id": pl.String,
        "failed_step": pl.String,
    }  # type: ignore[arg-type]
)

_STEP_SCHEMA = pl.Schema(
    {
        "run_id": pl.String,
        "correlation_id": pl.String,
        "attempt": pl.Int64,
        "step_run_id": pl.String,
        "step": pl.String,
        "started_at": pl.Datetime("us", "UTC"),
        "status": pl.String,
        "duration_ms": pl.Int64,
        "error": pl.String,
        "process_run_id": pl.String,
        "error_type": pl.String,
        "error_message": pl.String,
    }  # type: ignore[arg-type]
)


def _polars_schema(record: ExecutionRecord) -> pl.Schema:
    """Return the explicit Polars schema for *record*.

    Prevents Polars from inferring ``Null`` dtype for optional fields
    (``correlation_id``, ``error``) when their value is ``None``.

    Args:
        record: A completed execution record.

    Returns:
        Polars schema with fully-typed columns.

    Raises:
        TypeError: When *record* is not a recognised record type.
    """
    if isinstance(record, PipelineRunRecord):
        return _PIPELINE_SCHEMA
    if isinstance(record, ProcessRunRecord):
        return _PROCESS_SCHEMA
    if isinstance(record, StepRunRecord):
        return _STEP_SCHEMA
    raise TypeError(f"Unsupported execution record type: {type(record)!r}")


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------


class PolarsExecutionRecordWriter:
    """Write execution records through the Polars target writer ``append`` API.

    Uses an explicit per-record-type Polars schema so that optional fields
    (``correlation_id``, ``error``) are always written as nullable ``String``
    columns — never inferred as ``Null`` dtype, which Delta Lake rejects.

    Args:
        writer: A ``PolarsTargetWriter`` (or any object with an ``append``
                method compatible with that interface).
    """

    def __init__(self, writer: Any) -> None:
        self._writer = writer

    def write_record(self, record: ExecutionRecord, table_ref: TableRef, /) -> None:
        """Append one execution record row to *table_ref*.

        Args:
            record:    A completed pipeline, process, or step run record.
            table_ref: Destination table reference.
        """
        row = record_to_row(record)
        frame = pl.from_dicts([row], schema=_polars_schema(record)).lazy()
        self._writer.append(frame, table_ref, None)


__all__ = ["PolarsExecutionRecordWriter"]
