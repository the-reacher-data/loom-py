"""Execution record store contracts and implementations."""

from loom.etl.observability.stores.protocol import ExecutionRecordStore
from loom.etl.observability.stores.table import TableExecutionRecordStore

__all__ = ["ExecutionRecordStore", "TableExecutionRecordStore"]
