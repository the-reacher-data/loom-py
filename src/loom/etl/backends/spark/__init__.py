"""PySpark backend for the Loom ETL framework.

Public API:
- SparkSourceReader: SourceReader protocol implementation
- SparkTargetWriter: TargetWriter protocol implementation
"""

from loom.etl.backends.spark._catalog import SparkCatalog
from loom.etl.backends.spark._reader import SparkSourceReader
from loom.etl.backends.spark._schema import spark_apply_schema
from loom.etl.backends.spark._writer import SparkTargetWriter
from loom.etl.testing.spark import SparkTestSession

__all__ = [
    "SparkCatalog",
    "SparkSourceReader",
    "SparkTargetWriter",
    "SparkTestSession",
    "spark_apply_schema",
]
