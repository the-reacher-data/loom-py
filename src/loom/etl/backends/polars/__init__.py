"""Polars backend for the Loom ETL framework."""

from loom.etl.backends.polars._catalog import DeltaCatalog
from loom.etl.backends.polars._file_writer import PolarsFileWriter
from loom.etl.backends.polars._reader import PolarsSourceReader
from loom.etl.backends.polars._schema import SchemaNotFoundError, apply_schema
from loom.etl.backends.polars._writer import PolarsTargetWriter

__all__ = [
    "apply_schema",
    "DeltaCatalog",
    "PolarsFileWriter",
    "PolarsSourceReader",
    "PolarsTargetWriter",
    "SchemaNotFoundError",
]
