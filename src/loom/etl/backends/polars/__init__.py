"""Polars + Delta Lake backend for the Loom ETL framework.

Provides concrete implementations of the three I/O protocols:

* :class:`DeltaCatalog`      — :class:`~loom.etl._io.TableDiscovery`
* :class:`PolarsDeltaReader` — :class:`~loom.etl._io.SourceReader`
* :class:`PolarsDeltaWriter` — :class:`~loom.etl._io.TargetWriter`

Requires the ``etl-polars`` optional dependency group::

    pip install loom-kernel[etl-polars]

Usage::

    from pathlib import Path
    from loom.etl.backends.polars import DeltaCatalog, PolarsDeltaReader, PolarsDeltaWriter
    from loom.etl.compiler import ETLCompiler
    from loom.etl.executor import ETLExecutor

    root = Path("/data/delta")
    catalog = DeltaCatalog(root)
    executor = ETLExecutor(
        reader=PolarsDeltaReader(root),
        writer=PolarsDeltaWriter(root, catalog),
    )
    plan = ETLCompiler(catalog=catalog).compile_step(MyStep)
    executor.run_step(plan, MyParams(...))
"""

from loom.etl.backends.polars._catalog import DeltaCatalog
from loom.etl.backends.polars._reader import PolarsDeltaReader
from loom.etl.backends.polars._schema import SchemaError, SchemaNotFoundError, apply_schema
from loom.etl.backends.polars._writer import PolarsDeltaWriter

__all__ = [
    "DeltaCatalog",
    "PolarsDeltaReader",
    "PolarsDeltaWriter",
    "apply_schema",
    "SchemaNotFoundError",
    "SchemaError",
]
