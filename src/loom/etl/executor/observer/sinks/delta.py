"""DeltaRunSink — Delta-table RunSink powered by the active ETL backend writer.

Writes ETL run records to three Delta tables:

* ``pipeline_runs``  — one row per completed pipeline run.
* ``process_runs``   — one row per completed process run.
* ``step_runs``      — one row per completed step run.

This sink does not call ``write_deltalake`` directly.  It delegates to an
injected append writer so the same backend selected for the pipeline
(Polars/Spark) is also used for run persistence.
"""

from __future__ import annotations

from typing import Any

from loom.etl.executor.observer._events import (
    PipelineRunRecord,
    ProcessRunRecord,
    RunRecord,
    StepRunRecord,
)
from loom.etl.executor.observer.sinks._append_writer import RunSinkAppendWriter
from loom.etl.schema._table import TableRef

_TABLE_FOR: dict[type[Any], str] = {
    PipelineRunRecord: "pipeline_runs",
    ProcessRunRecord: "process_runs",
    StepRunRecord: "step_runs",
}


class DeltaRunSink:
    """Persist ETL run records to Delta Lake tables.

    Implements :class:`~loom.etl.executor.observer.sinks.RunSink`.

    Each :meth:`write` call appends one row to the corresponding Delta table
    through the injected append writer. Tables are created automatically on
    first write by the backend writer implementation.

    Args:
        writer:   Backend append writer implementation.
        database: Optional database/schema prefix for table names.
                  Example: ``"ops"`` writes to ``ops.pipeline_runs``,
                  ``ops.process_runs``, ``ops.step_runs``.

    Example::

        # Built automatically by ETLRunner.from_yaml / from_config.
        # Manual wiring is unusual and mostly useful for tests.
        sink = DeltaRunSink(writer=my_backend_append_writer, database="ops")
    """

    def __init__(self, writer: RunSinkAppendWriter, *, database: str = "") -> None:
        self._writer = writer
        self._database = database.strip()

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
        self._writer.append(record, _table_ref(self._database, table_name))


def _table_ref(database: str, table_name: str) -> TableRef:
    if database:
        return TableRef(f"{database}.{table_name}")
    return TableRef(table_name)
