"""Polars-backed execution record writer."""

from __future__ import annotations

import dataclasses
from typing import Any

import polars as pl
from polars.datatypes import DataTypeClass

from loom.etl.observability.records import (
    ExecutionRecord,
    PipelineRunRecord,
    ProcessRunRecord,
    StepRunRecord,
)
from loom.etl.schema._table import TableRef

_S: pl.DataType | DataTypeClass = pl.String
_I64: pl.DataType | DataTypeClass = pl.Int64
_TS: pl.DataType | DataTypeClass = pl.Datetime("us", "UTC")

_PIPELINE_SCHEMA = pl.Schema(
    {
        "run_id": _S,
        "correlation_id": _S,
        "attempt": _I64,
        "pipeline": _S,
        "started_at": _TS,
        "status": _S,
        "duration_ms": _I64,
        "error": _S,
        "error_type": _S,
        "error_message": _S,
        "failed_step_run_id": _S,
        "failed_step": _S,
    }
)
_PROCESS_SCHEMA = pl.Schema(
    {
        "run_id": _S,
        "correlation_id": _S,
        "attempt": _I64,
        "process_run_id": _S,
        "process": _S,
        "started_at": _TS,
        "status": _S,
        "duration_ms": _I64,
        "error": _S,
        "error_type": _S,
        "error_message": _S,
        "failed_step_run_id": _S,
        "failed_step": _S,
    }
)
_STEP_SCHEMA = pl.Schema(
    {
        "run_id": _S,
        "correlation_id": _S,
        "attempt": _I64,
        "step_run_id": _S,
        "step": _S,
        "started_at": _TS,
        "status": _S,
        "duration_ms": _I64,
        "error": _S,
        "process_run_id": _S,
        "error_type": _S,
        "error_message": _S,
    }
)

_SCHEMA_BY_TYPE: dict[type, pl.Schema] = {
    PipelineRunRecord: _PIPELINE_SCHEMA,
    ProcessRunRecord: _PROCESS_SCHEMA,
    StepRunRecord: _STEP_SCHEMA,
}


def _record_to_row(record: ExecutionRecord) -> dict[str, Any]:
    """Convert an execution record dataclass into a plain row mapping."""
    row = dataclasses.asdict(record)
    # Persist only snapshot fields in Delta tables; lifecycle event type is
    # still used by log observers but does not add analytical value here.
    row.pop("event", None)
    row["status"] = str(row["status"])
    return row


def _polars_schema(record: ExecutionRecord) -> pl.Schema:
    """Return the explicit Polars schema for *record*."""
    schema = _SCHEMA_BY_TYPE.get(type(record))
    if schema is None:
        raise TypeError(f"Unsupported execution record type: {type(record)!r}")
    return schema


class PolarsExecutionRecordWriter:
    """Write execution records through the Polars target writer ``append`` API.

    Uses explicit per-record-type schemas so all optional fields are typed as
    nullable ``String`` — never inferred as ``Null`` dtype, which Delta Lake rejects.

    Args:
        writer: Any object exposing an ``append(frame, table_ref, params)`` method.
    """

    def __init__(self, writer: Any) -> None:
        self._writer = writer

    def write_record(self, record: ExecutionRecord, table_ref: TableRef, /) -> None:
        """Append one execution record row to *table_ref*."""
        row = _record_to_row(record)
        frame = pl.from_dicts([row], schema=_polars_schema(record)).lazy()
        self._writer.append(frame, table_ref, None)


__all__ = ["PolarsExecutionRecordWriter"]
