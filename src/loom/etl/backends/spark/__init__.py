"""PySpark backend for the Loom ETL framework.

Provides Spark-specific implementations of the ETL I/O protocols:

* :class:`SparkSourceReader` — :class:`~loom.etl._io.SourceReader`
* :class:`SparkTargetWriter` — :class:`~loom.etl._io.TargetWriter`
* :class:`SparkCatalog` — :class:`~loom.etl._io.TableDiscovery` via Unity Catalog

Requires the ``etl-spark`` optional dependency group::

    pip install loom-kernel[etl-spark]

Unity Catalog (Databricks) — fully managed by Spark::

    from loom.etl.backends.spark import SparkCatalog, SparkSourceReader, SparkTargetWriter

    # locator=None → spark.table() / saveAsTable()
    catalog = SparkCatalog(spark)
    reader  = SparkSourceReader(spark)
    writer  = SparkTargetWriter(spark, None)

Path-based (S3, GCS, ADLS, DBFS)::

    from loom.etl.backends.spark import SparkSourceReader, SparkTargetWriter

    reader = SparkSourceReader(spark, "s3://my-lake/")
    writer = SparkTargetWriter(spark, "s3://my-lake/")
"""

from loom.etl.backends.spark._catalog import SparkCatalog
from loom.etl.backends.spark._schema import spark_apply_schema
from loom.etl.backends.spark.reader import SparkSourceReader
from loom.etl.backends.spark.writer import SparkTargetWriter
from loom.etl.testing.spark import SparkTestSession

# Compatibility aliases kept intentionally as equivalent names.
SparkDeltaReader = SparkSourceReader
SparkDeltaWriter = SparkTargetWriter

__all__ = [
    "SparkCatalog",
    "SparkSourceReader",
    "SparkTargetWriter",
    "SparkDeltaReader",
    "SparkDeltaWriter",
    "SparkTestSession",
    "spark_apply_schema",
]
