"""Polars physical schema types, Delta schema reader, and schema alignment.

Schema enforcement rules
------------------------

+--------------------------+----------+----------+-----------+
| Situation                | STRICT   | EVOLVE   | OVERWRITE |
+==========================+==========+==========+===========+
| Type mismatch            | cast     | cast     | —         |
+--------------------------+----------+----------+-----------+
| Extra col in frame       | dropped  | kept     | —         |
+--------------------------+----------+----------+-----------+
| Col missing from frame   | null     | null     | —         |
+--------------------------+----------+----------+-----------+
| schema is None           | error    | error    | passthru  |
+--------------------------+----------+----------+-----------+

StructType — field-by-field casting
-------------------------------------
For :class:`~polars.Struct` columns the cast descends field by field using
``pl.Expr.struct.field()``, then reconstructs the struct with
``pl.struct([...])``.  This correctly handles arbitrarily nested structures
such as ``Struct[Struct[...]]`` at any depth.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TypeGuard

import polars as pl
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError
from polars.datatypes import DataTypeClass as _DataTypeClass

from loom.etl.declarative.target import SchemaMode
from loom.etl.schema._schema import SchemaNotFoundError

_log = logging.getLogger(__name__)

__all__ = [
    "PolarsPhysicalSchema",
    "SchemaNotFoundError",
    "apply_schema_polars",
    "read_delta_physical_schema",
]


@dataclass(frozen=True)
class PolarsPhysicalSchema:
    """Physical schema snapshot for a resolved Polars target.

    Args:
        schema: Native Polars schema for write-time alignment.
        partition_columns: Ordered partition columns.
    """

    schema: pl.Schema
    partition_columns: tuple[str, ...] = ()


# ---------------------------------------------------------------------------
# apply_schema — conform frame to destination schema
# ---------------------------------------------------------------------------


def apply_schema_polars(
    frame: pl.LazyFrame,
    schema: pl.Schema | None,
    mode: SchemaMode,
) -> pl.LazyFrame:
    """Conform *frame* to the destination table's *schema* according to *mode*.

    Args:
        frame:  Lazy frame produced by the step's ``execute()``.
        schema: Destination table's native Polars schema.  May be ``None``
                only when *mode* is ``OVERWRITE``.
        mode:   Schema enforcement strategy.

    Returns:
        Frame with columns cast to destination types.  ``STRICT`` additionally
        drops columns absent from *schema* and selects in schema order.
        ``EVOLVE`` keeps extra frame columns unchanged.
        ``OVERWRITE`` returns the frame unchanged.

    Raises:
        SchemaNotFoundError: When *schema* is ``None`` and *mode* is not
                             ``OVERWRITE`` (table does not yet exist).
    """
    if mode is SchemaMode.OVERWRITE:
        return frame

    if schema is None:
        raise SchemaNotFoundError(
            "Destination table does not yet exist. "
            "Write with SchemaMode.OVERWRITE to create it on first run."
        )

    frame_cols = set(frame.collect_schema().names())
    present = [(name, dtype) for name, dtype in schema.items() if name in frame_cols]
    missing = [(name, dtype) for name, dtype in schema.items() if name not in frame_cols]

    _log.debug(
        "apply_schema mode=%s present=%d missing=%d",
        mode,
        len(present),
        len(missing),
    )

    if present:
        cast_exprs = [_cast_expr(pl.col(name), dtype).alias(name) for name, dtype in present]
        frame = frame.with_columns(cast_exprs)

    if missing:
        null_exprs = [_null_expr(dtype).alias(name) for name, dtype in missing]
        frame = frame.with_columns(null_exprs)

    if mode is SchemaMode.STRICT:
        frame = frame.select(list(schema.keys()))

    return frame


def _cast_expr(expr: pl.Expr, dtype: pl.DataType | _DataTypeClass) -> pl.Expr:
    """Recursively cast *expr* to *dtype*.

    For :class:`~polars.Struct` descends field by field so that nested structs
    are handled correctly at any depth.  All other types use ``expr.cast()``
    directly — Polars preserves full type information including inner types for
    ``List``, ``Array``, and ``Decimal``.
    """
    if isinstance(dtype, pl.Struct):
        field_exprs = [
            _cast_expr(expr.struct.field(f.name), f.dtype).alias(f.name) for f in dtype.fields
        ]
        return pl.struct(field_exprs)
    return expr.cast(dtype)


def _null_expr(dtype: pl.DataType | _DataTypeClass) -> pl.Expr:
    """Return a typed-null expression for *dtype*.

    Delegates to ``pl.lit(None).cast(dtype)`` for all types — Polars preserves
    exact type parameters (timezone, inner element type, struct fields,
    precision) and produces a null value of the correct dtype, including null
    structs and null lists.
    """
    return pl.lit(None).cast(dtype)


# ---------------------------------------------------------------------------
# Delta schema reading (private helpers)
# ---------------------------------------------------------------------------

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
) -> PolarsPhysicalSchema | None:
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
    schema_json = _delta_schema_to_json(dt.schema())
    return PolarsPhysicalSchema(
        schema=_delta_json_to_polars_schema(schema_json),
        partition_columns=tuple(dt.metadata().partition_columns),
    )


def _delta_schema_to_json(raw_schema: object) -> object:
    """Extract JSON-serializable schema from DeltaTable schema object."""
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
    """Convert Delta JSON schema to Polars Schema."""
    if not isinstance(raw_schema, Mapping):
        raise TypeError(f"Unsupported Delta schema payload: {type(raw_schema)!r}")

    root_type = raw_schema.get("type")
    if root_type != "struct":
        raise TypeError(f"Delta root schema must be struct, got {root_type!r}")

    fields = raw_schema.get("fields")
    return pl.Schema(_struct_fields_to_dict(fields))


def _struct_fields_to_dict(raw_fields: object) -> dict[str, pl.DataType]:
    """Convert Delta struct fields to Polars field dict."""
    if not _is_sequence(raw_fields):
        raise TypeError(f"Delta struct fields must be a sequence, got {type(raw_fields)!r}")

    fields: dict[str, pl.DataType] = {}
    for raw_field in raw_fields:
        name, dtype = _parse_field(raw_field)
        fields[name] = dtype
    return fields


def _parse_field(raw_field: object) -> tuple[str, pl.DataType]:
    """Parse a single Delta field into (name, dtype)."""
    if not isinstance(raw_field, Mapping):
        raise TypeError(f"Delta field must be a mapping, got {type(raw_field)!r}")
    name = raw_field.get("name")
    if not isinstance(name, str) or not name:
        raise TypeError(f"Delta field name must be non-empty str, got {name!r}")
    return name, _delta_type_to_polars(raw_field.get("type"))


def _delta_type_to_polars(raw_type: object) -> pl.DataType:
    """Convert Delta type to Polars DataType."""
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
