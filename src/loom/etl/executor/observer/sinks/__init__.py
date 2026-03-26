"""Run sink implementations — persistence backends for ETL run records.

Contracts:

* :class:`RunSink` — persistence protocol.

Implementations:

* :class:`DeltaRunSink` — appends run records to Delta Lake tables.
"""

from loom.etl.executor.observer.sinks.delta import DeltaRunSink
from loom.etl.executor.observer.sinks.protocol import RunSink

__all__ = [
    "RunSink",
    "DeltaRunSink",
]
