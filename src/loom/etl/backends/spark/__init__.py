"""PySpark + Delta Lake backend for the Loom ETL framework.

Provides Spark-specific implementations of the two I/O protocols:

* :class:`SparkDeltaReader` — :class:`~loom.etl._io.SourceReader`
* :class:`SparkDeltaWriter` — :class:`~loom.etl._io.TargetWriter`

For catalog discovery, reuse :class:`~loom.etl.backends.polars.DeltaCatalog`
— it reads Delta log metadata via the Python ``deltalake`` package and works
equally well with tables written by Spark or Polars.

Requires the ``etl-spark`` optional dependency group::

    pip install loom-kernel[etl-spark]

Usage::

    from pathlib import Path
    from pyspark.sql import SparkSession
    from loom.etl.backends.polars import DeltaCatalog
    from loom.etl.backends.spark import SparkDeltaReader, SparkDeltaWriter
    from loom.etl.compiler import ETLCompiler
    from loom.etl.executor import ETLExecutor

    root = Path("/data/delta")
    catalog = DeltaCatalog(root)
    executor = ETLExecutor(
        reader=SparkDeltaReader(spark, root),
        writer=SparkDeltaWriter(spark, root, catalog),
    )
    plan = ETLCompiler(catalog=catalog).compile_step(MyStep)
    executor.run_step(plan, MyParams(...))
"""

from loom.etl.backends.spark._reader import SparkDeltaReader
from loom.etl.backends.spark._schema import spark_apply_schema
from loom.etl.backends.spark._writer import SparkDeltaWriter

__all__ = [
    "SparkDeltaReader",
    "SparkDeltaWriter",
    "spark_apply_schema",
]
