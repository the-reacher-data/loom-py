"""PySpark backend for the Loom ETL framework.

Public API:
- SparkSourceReader: SourceReader protocol implementation
- SparkTargetWriter: TargetWriter protocol implementation
"""

from loom.etl.backends.spark._reader import SparkSourceReader
from loom.etl.backends.spark._schema import apply_schema_spark
from loom.etl.backends.spark._writer import SparkTargetWriter
from loom.etl.testing.spark import SparkTestSession

__all__ = [
    "SparkSourceReader",
    "SparkTargetWriter",
    "SparkTestSession",
    "apply_schema_spark",
]
