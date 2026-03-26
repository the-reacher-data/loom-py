"""PySpark + Delta Lake backend for the Loom ETL framework.

Provides Spark-specific implementations of the ETL I/O protocols:

* :class:`SparkDeltaReader` — :class:`~loom.etl._io.SourceReader`
* :class:`SparkDeltaWriter` — :class:`~loom.etl._io.TargetWriter`
* :class:`SparkCatalog` — :class:`~loom.etl._io.TableDiscovery` via Unity Catalog

Requires the ``etl-spark`` optional dependency group::

    pip install loom-kernel[etl-spark]

Unity Catalog (Databricks) — fully managed by Spark::

    from loom.etl.backends.spark import SparkCatalog, SparkDeltaReader, SparkDeltaWriter

    # locator=None → spark.table() / saveAsTable()
    catalog = SparkCatalog(spark)
    reader  = SparkDeltaReader(spark)
    writer  = SparkDeltaWriter(spark, None, catalog)

Path-based (S3, GCS, ADLS, DBFS) — pair with DeltaCatalog::

    from loom.etl.backends.spark import SparkDeltaReader, SparkDeltaWriter
    from loom.etl.backends.polars import DeltaCatalog

    catalog = DeltaCatalog("s3://my-lake/")
    reader  = SparkDeltaReader(spark, "s3://my-lake/")
    writer  = SparkDeltaWriter(spark, "s3://my-lake/", catalog)
"""

from loom.etl.backends.spark._catalog import SparkCatalog
from loom.etl.backends.spark._reader import SparkDeltaReader
from loom.etl.backends.spark._schema import spark_apply_schema
from loom.etl.backends.spark._writer import SparkDeltaWriter
from loom.etl.testing.spark import SparkTestSession

__all__ = [
    "SparkCatalog",
    "SparkDeltaReader",
    "SparkDeltaWriter",
    "SparkTestSession",
    "spark_apply_schema",
]
