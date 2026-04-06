"""Internal protocol for appending run records to backend tables."""

from __future__ import annotations

from typing import Protocol

from loom.etl.executor.observer._events import RunRecord
from loom.etl.schema._table import TableRef


class RunSinkAppendWriter(Protocol):
    """Append one run record into the backend table identified by ``table_ref``."""

    def append(self, record: RunRecord, table_ref: TableRef, /) -> None:
        """Persist *record* as one appended row in *table_ref*."""
        ...
