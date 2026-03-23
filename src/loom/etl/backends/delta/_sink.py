"""DeltaRunSink — Delta Lake implementation of RunSink.

Writes ETL run records to three Delta tables under a configurable root:

* ``pipeline_runs/``  — one row per completed pipeline run.
* ``process_runs/``   — one row per completed process run.
* ``step_runs/``      — one row per completed step run.

Records are appended with ``schema_mode="merge"`` so new fields added to
record types are automatically handled without a table migration.

Requires the ``etl-polars`` extra (``polars`` + ``deltalake``).
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any

import polars as pl
from deltalake import write_deltalake

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

    Implements :class:`~loom.etl.executor.RunSink`.

    Each :meth:`write` call appends one row to the corresponding Delta table
    under *root*.  Tables are created automatically on first write.

    Args:
        root: Filesystem root for the three Delta tables.

    Example::

        from pathlib import Path
        from loom.etl.backends.delta import DeltaRunSink
        from loom.etl.executor import ETLExecutor, RunSinkObserver

        sink     = DeltaRunSink(root=Path("/data/etl_runs"))
        executor = ETLExecutor(reader, writer, observers=[RunSinkObserver(sink)])
    """

    def __init__(self, root: Path) -> None:
        self._root = root

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

        path = self._root / table_name
        path.mkdir(parents=True, exist_ok=True)

        row = dataclasses.asdict(record)
        df = pl.DataFrame([row])
        write_deltalake(str(path), df, mode="append", schema_mode="merge")
