"""PySpark backend for the Loom ETL framework.

Public API:
- SparkSourceReader: SourceReader protocol implementation
- SparkTargetWriter: TargetWriter protocol implementation
- SparkBackend: Unified backend with read/schema/write operations
"""

from loom.etl.backends.spark._backend import (
    SparkBackend,
    SparkReadOps,
    SparkSchemaOps,
    SparkWriteOps,
)
from loom.etl.backends.spark._catalog import SparkCatalog
from loom.etl.backends.spark._schema import spark_apply_schema
from loom.etl.backends.spark.io import SparkSourceReader, SparkTargetWriter
from loom.etl.testing.spark import SparkTestSession

__all__ = [
    "SparkBackend",
    "SparkCatalog",
    "SparkReadOps",
    "SparkSchemaOps",
    "SparkSourceReader",
    "SparkTargetWriter",
    "SparkTestSession",
    "SparkWriteOps",
    "spark_apply_schema",
]
