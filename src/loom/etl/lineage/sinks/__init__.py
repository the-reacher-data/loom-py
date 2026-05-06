"""Persistence sinks for ETL lineage records."""

from loom.etl.lineage.sinks._protocol import (
    LineageStore,
    LineageWriter,
    RecordFrameTargetWriter,
)
from loom.etl.lineage.sinks._table import TableLineageStore
from loom.etl.lineage.sinks._writer import TargetLineageWriter

__all__ = [
    "LineageStore",
    "LineageWriter",
    "RecordFrameTargetWriter",
    "TargetLineageWriter",
    "TableLineageStore",
]
