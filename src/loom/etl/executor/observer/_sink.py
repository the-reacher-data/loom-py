"""RunSink — persistence protocol for ETL run records.

Implementations decide where and how records are written (Delta, S3, RDBMS, …).
The observer layer only depends on this protocol — never on a concrete backend.

Internal module — not part of the public API.
"""

from __future__ import annotations

from typing import Protocol

from loom.etl.executor.observer._events import RunRecord


class RunSink(Protocol):
    """Protocol for persisting ETL run records.

    Each call to :meth:`write` persists one completed run record.
    The implementation routes the record to the appropriate storage
    (Delta table, file, database row, …) based on the record type.

    Example::

        sink = DeltaRunSink(root=Path("/data/runs"))
        observer = RunSinkObserver(sink)
        executor = ETLExecutor(reader, writer, observers=[observer])
    """

    def write(self, record: RunRecord) -> None:
        """Persist *record* to the underlying storage.

        Args:
            record: A completed :class:`~loom.etl.executor.PipelineRunRecord`,
                    :class:`~loom.etl.executor.ProcessRunRecord`, or
                    :class:`~loom.etl.executor.StepRunRecord`.
        """
        ...
