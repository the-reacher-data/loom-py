"""DeltaRunSink — Delta Lake implementation of RunSink.

Writes ETL run records to three Delta tables under a configurable
:class:`~loom.etl._locator.TableLocation`:

* ``pipeline_runs/``  — one row per completed pipeline run.
* ``process_runs/``   — one row per completed process run.
* ``step_runs/``      — one row per completed step run.

Records are appended with ``schema_mode="merge"`` so new fields added to
record types are handled automatically without a table migration.

Requires the ``etl-polars`` extra (``polars`` + ``deltalake``).

Storage options are forwarded verbatim to delta-rs — pass any key accepted
by the target object store (AWS, GCS, Azure).
See https://delta-io.github.io/delta-rs/api/delta_writer/
"""

from __future__ import annotations

import dataclasses
import os
from typing import Any

import polars as pl
from deltalake import write_deltalake

from loom.etl._locator import TableLocation, _as_location
from loom.etl.executor.observer._events import (
    PipelineRunRecord,
    ProcessRunRecord,
    RunRecord,
    StepRunRecord,
)

_TABLE_FOR: dict[type[Any], str] = {
    PipelineRunRecord: "pipeline_runs",
    ProcessRunRecord: "process_runs",
    StepRunRecord: "step_runs",
}


class DeltaRunSink:
    """Persist ETL run records to Delta Lake tables.

    Implements :class:`~loom.etl.executor.observer.sinks.RunSink`.

    Each :meth:`write` call appends one row to the corresponding Delta table
    under *location*.  Tables are created automatically on first write.
    No local directory creation is performed — the URI is passed directly
    to delta-rs, which supports local paths, S3, GCS, and Azure.

    Args:
        location: Physical storage address for the run tables.  Accepts a
                  URI string (e.g. ``"s3://my-lake/runs/"``), a
                  :class:`pathlib.Path`, or a fully configured
                  :class:`~loom.etl._locator.TableLocation`.  The table name
                  (e.g. ``pipeline_runs``) is appended to the URI at write time.

    Example::

        from loom.etl import ETLRunner
        from loom.etl.executor import RunSinkObserver
        from loom.etl.executor.observer.sinks import DeltaRunSink

        # Simple — credentials from environment
        sink = DeltaRunSink("s3://my-lake/runs/")

        # With explicit credentials
        from loom.etl import TableLocation
        sink = DeltaRunSink(
            TableLocation(uri="s3://my-lake/runs/", storage_options={"AWS_REGION": "eu-west-1"})
        )

        runner = ETLRunner.from_yaml("loom.yaml", observers=[RunSinkObserver(sink)])
    """

    def __init__(self, location: str | os.PathLike[str] | TableLocation) -> None:
        self._location = _as_location(location)

    def write(self, record: RunRecord) -> None:
        """Append *record* to the appropriate Delta table.

        Args:
            record: A completed pipeline, process, or step run record.

        Raises:
            TypeError: If *record* is not a recognised record type.
        """
        table_name = _TABLE_FOR.get(type(record))
        if table_name is None:
            raise TypeError(f"DeltaRunSink: unrecognised record type {type(record)!r}")

        uri = f"{self._location.uri.rstrip('/')}/{table_name}"
        row = dataclasses.asdict(record)
        df = pl.DataFrame([row])
        write_deltalake(
            uri,
            df,
            mode="append",
            schema_mode="merge",
            storage_options=self._location.storage_options or None,
        )
