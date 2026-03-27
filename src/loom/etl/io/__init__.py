"""I/O declaration API for ETL sources, targets, and file formats."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS: dict[str, str] = {
    "Format": "loom.etl.io._format",
    "ReadOptions": "loom.etl.io._read_options",
    "CsvReadOptions": "loom.etl.io._read_options",
    "JsonReadOptions": "loom.etl.io._read_options",
    "ExcelReadOptions": "loom.etl.io._read_options",
    "ParquetReadOptions": "loom.etl.io._read_options",
    "WriteOptions": "loom.etl.io._write_options",
    "CsvWriteOptions": "loom.etl.io._write_options",
    "ParquetWriteOptions": "loom.etl.io._write_options",
    "FromTable": "loom.etl.io._source",
    "FromFile": "loom.etl.io._source",
    "FromTemp": "loom.etl.io._source",
    "Sources": "loom.etl.io._source",
    "SourceSet": "loom.etl.io._source",
    "IntoTable": "loom.etl.io._target",
    "IntoFile": "loom.etl.io._target",
    "IntoTemp": "loom.etl.io._target",
    "SchemaMode": "loom.etl.io._target",
}

__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    module_path = _EXPORTS.get(name)
    if module_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_path)
    value = getattr(module, name)
    globals()[name] = value
    return value
