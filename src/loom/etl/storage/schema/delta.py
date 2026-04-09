"""delta-rs-backed physical schema reader."""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping, Sequence
from typing import TypeGuard

import polars as pl
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from loom.etl.storage.routing import CatalogTarget, PathTarget, ResolvedTarget
from loom.etl.storage.schema import PhysicalSchema, PolarsPhysicalSchema


class DeltaSchemaReader:
    """Read physical schema from Delta log for path targets."""

    def read_schema(self, target: ResolvedTarget) -> PhysicalSchema | None:
        """Return physical schema for *target*, or ``None`` if absent."""
        if isinstance(target, CatalogTarget):
            raise ValueError(
                "DeltaSchemaReader only supports path targets; "
                f"got catalog target for {target.logical_ref.ref!r}."
            )
        if not isinstance(target, PathTarget):
            raise TypeError(f"Unsupported resolved target: {type(target)!r}")

        return read_delta_physical_schema(
            target.location.uri,
            target.location.storage_options,
        )


_DECIMAL_TYPE_RE = re.compile(r"^decimal\((?P<precision>\d+),(?P<scale>\d+)\)$")
_PRIMITIVE_TYPES: dict[str, Callable[[], pl.DataType]] = {
    "byte": pl.Int8,
    "short": pl.Int16,
    "integer": pl.Int32,
    "long": pl.Int64,
    "float": pl.Float32,
    "double": pl.Float64,
    "boolean": pl.Boolean,
    "string": pl.String,
    "binary": pl.Binary,
    "date": pl.Date,
    "timestamp": lambda: pl.Datetime("us", "UTC"),
    "timestamp_ntz": lambda: pl.Datetime("us"),
    "null": pl.Null,
    "void": pl.Null,
}


def read_delta_physical_schema(
    uri: str,
    storage_options: dict[str, str] | None = None,
) -> PhysicalSchema | None:
    """Read physical schema directly from a Delta table URI.

    Args:
        uri: Delta table URI/path (including ``uc://catalog.schema.table``).
        storage_options: Optional delta-rs storage options.

    Returns:
        Physical schema when the table exists, else ``None``.
    """
    try:
        dt = DeltaTable(uri, storage_options=storage_options or None)
    except TableNotFoundError:
        return None
    schema_json = _delta_schema_json(dt.schema())
    return PolarsPhysicalSchema(
        schema=_delta_json_to_polars_schema(schema_json),
        partition_columns=tuple(dt.metadata().partition_columns),
    )


def _delta_schema_json(raw_schema: object) -> object:
    to_json_method = getattr(raw_schema, "to_json", None)
    if callable(to_json_method):
        payload = to_json_method()
        if isinstance(payload, str):
            return json.loads(payload)

    json_method = getattr(raw_schema, "json", None)
    if not callable(json_method):
        raise TypeError(f"Unsupported Delta schema object: {type(raw_schema)!r}")
    return json_method()


def _delta_json_to_polars_schema(raw_schema: object) -> pl.Schema:
    if not isinstance(raw_schema, Mapping):
        raise TypeError(f"Unsupported Delta schema payload: {type(raw_schema)!r}")

    root_type = raw_schema.get("type")
    if root_type != "struct":
        raise TypeError(f"Delta root schema must be struct, got {root_type!r}")

    fields = raw_schema.get("fields")
    return pl.Schema(_struct_fields_to_dict(fields))


def _struct_fields_to_dict(raw_fields: object) -> dict[str, pl.DataType]:
    if not _is_sequence(raw_fields):
        raise TypeError(f"Delta struct fields must be a sequence, got {type(raw_fields)!r}")

    fields: dict[str, pl.DataType] = {}
    for raw_field in raw_fields:
        name, dtype = _parse_field(raw_field)
        fields[name] = dtype
    return fields


def _parse_field(raw_field: object) -> tuple[str, pl.DataType]:
    if not isinstance(raw_field, Mapping):
        raise TypeError(f"Delta field must be a mapping, got {type(raw_field)!r}")
    name = raw_field.get("name")
    if not isinstance(name, str) or not name:
        raise TypeError(f"Delta field name must be non-empty str, got {name!r}")
    return name, _delta_type_to_polars(raw_field.get("type"))


def _delta_type_to_polars(raw_type: object) -> pl.DataType:
    if isinstance(raw_type, str):
        primitive = _PRIMITIVE_TYPES.get(raw_type)
        if primitive is not None:
            return primitive()
        decimal_match = _DECIMAL_TYPE_RE.match(raw_type)
        if decimal_match is not None:
            precision = int(decimal_match.group("precision"))
            scale = int(decimal_match.group("scale"))
            return pl.Decimal(precision=precision, scale=scale)
        raise TypeError(f"Unsupported Delta primitive type: {raw_type!r}")

    if not isinstance(raw_type, Mapping):
        raise TypeError(f"Delta type node must be str or mapping, got {type(raw_type)!r}")

    node_type = raw_type.get("type")
    if node_type == "array":
        return pl.List(_delta_type_to_polars(raw_type.get("elementType")))
    if node_type == "struct":
        return pl.Struct(_struct_fields_to_dict(raw_type.get("fields")))
    if node_type == "map":
        key_type = _delta_type_to_polars(raw_type.get("keyType"))
        value_type = _delta_type_to_polars(raw_type.get("valueType"))
        return pl.List(pl.Struct({"key": key_type, "value": value_type}))
    raise TypeError(f"Unsupported Delta nested type node: {node_type!r}")


def _is_sequence(value: object) -> TypeGuard[Sequence[object]]:
    return isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray)
