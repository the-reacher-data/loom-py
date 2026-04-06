"""apply_schema — conform a Polars LazyFrame to the destination table's native schema.

Casts each present column to the declared Polars dtype, fills missing columns
with typed nulls, and optionally selects the exact schema column set depending
on :class:`~loom.etl.io.target.SchemaMode`.

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

OVERWRITE passes the frame through unchanged — the writer replaces the table
schema with whatever the frame contains.  ``schema`` may be ``None`` (table
does not yet exist) without raising.

StructType — field-by-field casting
-------------------------------------
For :class:`~polars.Struct` columns the cast descends field by field using
``pl.Expr.struct.field()``, then reconstructs the struct with
``pl.struct([...])``.  This correctly handles arbitrarily nested structures
such as ``Struct[Struct[...]]`` at any depth.  All other types (``List``,
``Array``, ``Decimal``, ``Datetime``, etc.) use ``expr.cast()`` directly —
Polars preserves full type parameters including inner element types, timezone,
and precision.
"""

from __future__ import annotations

import logging

import polars as pl
from polars.datatypes import DataTypeClass as _DataTypeClass

from loom.etl.io.target import SchemaMode
from loom.etl.schema._schema import SchemaNotFoundError

_log = logging.getLogger(__name__)

__all__ = ["apply_schema", "SchemaNotFoundError"]


def apply_schema(
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


# ---------------------------------------------------------------------------
# Cast expression builder
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Null column builder
# ---------------------------------------------------------------------------


def _null_expr(dtype: pl.DataType | _DataTypeClass) -> pl.Expr:
    """Return a typed-null expression for *dtype*.

    Delegates to ``pl.lit(None).cast(dtype)`` for all types — Polars preserves
    exact type parameters (timezone, inner element type, struct fields,
    precision) and produces a null value of the correct dtype, including null
    structs and null lists.
    """
    return pl.lit(None).cast(dtype)
