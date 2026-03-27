"""Schema, contracts, and table-reference primitives for ETL."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS: dict[str, str] = {
    "SchemaContract": "loom.etl.schema._contract",
    "JsonContract": "loom.etl.schema._contract",
    "resolve_schema": "loom.etl.schema._contract",
    "resolve_json_type": "loom.etl.schema._contract",
    "ColumnSchema": "loom.etl.schema._schema",
    "LoomDtype": "loom.etl.schema._schema",
    "LoomType": "loom.etl.schema._schema",
    "SchemaNotFoundError": "loom.etl.schema._schema",
    "SchemaError": "loom.etl.schema._schema",
    "ListType": "loom.etl.schema._schema",
    "ArrayType": "loom.etl.schema._schema",
    "StructType": "loom.etl.schema._schema",
    "StructField": "loom.etl.schema._schema",
    "DecimalType": "loom.etl.schema._schema",
    "DatetimeType": "loom.etl.schema._schema",
    "DurationType": "loom.etl.schema._schema",
    "CategoricalType": "loom.etl.schema._schema",
    "EnumType": "loom.etl.schema._schema",
    "TableRef": "loom.etl.schema._table",
    "col": "loom.etl.schema._table",
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
