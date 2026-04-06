"""RunSink — persistence protocol for ETL run records.

Implementations decide where and how records are written (Delta, S3, RDBMS, …).
The observer layer depends only on this protocol — never on a concrete backend.
"""

from __future__ import annotations

from typing import Protocol

from loom.etl.executor.observer._events import RunRecord


class RunSink(Protocol):
    """Protocol for persisting ETL run records.

    Each :meth:`write` call persists one completed run record.  The
    implementation routes the record to the appropriate storage
    (Delta table, file, database row, …) based on the record type.

    Example::

        from loom.etl import ETLRunner

        # Configure ``observability.run_sink`` in YAML and let ETLRunner
        # build the sink with the active backend automatically.
        runner = ETLRunner.from_yaml("loom.yaml")
    """

    def write(self, record: RunRecord) -> None:
        """Persist *record* to the underlying storage.

        Args:
            record: A completed :class:`~loom.etl.executor.PipelineRunRecord`,
                    :class:`~loom.etl.executor.ProcessRunRecord`, or
                    :class:`~loom.etl.executor.StepRunRecord`.
        """
        ...
