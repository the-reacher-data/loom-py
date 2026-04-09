"""Polars backend for the Loom ETL framework."""

from loom.etl.backends.polars._backend import (
    PolarsBackend,
    PolarsReadOps,
    PolarsSchemaOps,
    PolarsWriteOps,
)
from loom.etl.backends.polars._catalog import DeltaCatalog
from loom.etl.backends.polars._file_writer import PolarsFileWriter
from loom.etl.backends.polars._schema import SchemaNotFoundError, apply_schema
from loom.etl.backends.polars.io import PolarsSourceReader, PolarsTargetWriter

__all__ = [
    "DeltaCatalog",
    "PolarsBackend",
    "PolarsFileWriter",
    "PolarsReadOps",
    "PolarsSchemaOps",
    "PolarsSourceReader",
    "PolarsTargetWriter",
    "PolarsWriteOps",
    "apply_schema",
    "SchemaNotFoundError",
]
