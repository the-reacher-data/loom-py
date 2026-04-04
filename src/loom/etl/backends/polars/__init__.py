"""Polars backend for the Loom ETL framework.

Provides concrete implementations of the three I/O protocols:

* :class:`DeltaCatalog`      — :class:`~loom.etl._io.TableDiscovery`
* :class:`PolarsSourceReader` — :class:`~loom.etl._io.SourceReader`
* :class:`PolarsTargetWriter` — :class:`~loom.etl._io.TargetWriter`

Requires the ``etl-polars`` optional dependency group::

    pip install loom-kernel[etl-polars]

Usage::

    from loom.etl.storage._locator import PrefixLocator
    from loom.etl.backends.polars import DeltaCatalog, PolarsSourceReader, PolarsTargetWriter
    from loom.etl.compiler import ETLCompiler
    from loom.etl.executor import ETLExecutor

    locator = PrefixLocator("s3://my-lake/", storage_options={"AWS_REGION": "eu-west-1"})
    catalog = DeltaCatalog(locator)
    executor = ETLExecutor(
        reader=PolarsSourceReader(locator),
        writer=PolarsTargetWriter(locator),
    )
    plan = ETLCompiler(catalog=catalog).compile_step(MyStep)
    executor.run_step(plan, MyParams(...))
"""

from loom.etl.backends.polars._catalog import DeltaCatalog
from loom.etl.backends.polars._schema import SchemaNotFoundError, apply_schema
from loom.etl.backends.polars.reader import PolarsSourceReader
from loom.etl.backends.polars.writer import PolarsTargetWriter

# Compatibility aliases kept intentionally as equivalent names.
PolarsDeltaReader = PolarsSourceReader
PolarsDeltaWriter = PolarsTargetWriter

__all__ = [
    "DeltaCatalog",
    "PolarsSourceReader",
    "PolarsTargetWriter",
    "PolarsDeltaReader",
    "PolarsDeltaWriter",
    "apply_schema",
    "SchemaNotFoundError",
]
