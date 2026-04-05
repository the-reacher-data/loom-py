"""I/O declaration API for ETL sources, targets, and file formats."""

from __future__ import annotations

from ._format import Format
from ._read_options import (
    CsvReadOptions,
    ExcelReadOptions,
    JsonReadOptions,
    ParquetReadOptions,
    ReadOptions,
)
from ._source import FromFile, FromTable, FromTemp, Sources, SourceSet
from ._target import IntoFile, IntoTable, IntoTemp, SchemaMode
from ._write_options import CsvWriteOptions, JsonWriteOptions, ParquetWriteOptions, WriteOptions

__all__ = [
    "Format",
    "ReadOptions",
    "CsvReadOptions",
    "JsonReadOptions",
    "ExcelReadOptions",
    "ParquetReadOptions",
    "WriteOptions",
    "CsvWriteOptions",
    "JsonWriteOptions",
    "ParquetWriteOptions",
    "FromTable",
    "FromFile",
    "FromTemp",
    "Sources",
    "SourceSet",
    "IntoTable",
    "IntoFile",
    "IntoTemp",
    "SchemaMode",
]
