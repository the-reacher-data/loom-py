"""Runtime contracts used by executor and backend implementations."""

from loom.etl.runtime.contracts import SourceReader, TableDiscovery, TargetWriter

__all__ = ["TableDiscovery", "SourceReader", "TargetWriter"]
