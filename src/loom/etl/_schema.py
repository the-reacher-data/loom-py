"""Loom canonical schema types — backend-agnostic column definitions.

These types form the schema contract between the ETL framework and any
catalog or writer implementation.  They carry no dependency on Polars,
Spark, or any specific storage backend.

Each backend translates :class:`LoomDtype` values to its native type system:

* Polars:  ``LoomDtype.INT64``  →  ``pl.Int64``
* Spark:   ``LoomDtype.INT64``  →  ``LongType()``
* Arrow:   ``LoomDtype.INT64``  →  ``pa.int64()``
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class LoomDtype(StrEnum):
    """Canonical data types understood by loom's schema system.

    Values match Polars naming conventions to minimise translation friction
    in the Polars backend, while remaining backend-agnostic at the protocol
    level.
    """

    # --- integer ---
    INT8 = "Int8"
    INT16 = "Int16"
    INT32 = "Int32"
    INT64 = "Int64"
    UINT8 = "UInt8"
    UINT16 = "UInt16"
    UINT32 = "UInt32"
    UINT64 = "UInt64"

    # --- floating point ---
    FLOAT32 = "Float32"
    FLOAT64 = "Float64"
    DECIMAL = "Decimal"

    # --- string / binary ---
    UTF8 = "Utf8"
    BINARY = "Binary"

    # --- boolean ---
    BOOLEAN = "Boolean"

    # --- temporal ---
    DATE = "Date"
    DATETIME = "Datetime"
    DURATION = "Duration"
    TIME = "Time"

    # --- null / unknown ---
    NULL = "Null"


@dataclass(frozen=True)
class ColumnSchema:
    """Backend-agnostic definition of a single table column.

    Used by :class:`~loom.etl._io.TableDiscovery` to describe table schemas
    and by backend writers to align frames before writing.

    Args:
        name:     Column name.
        dtype:    Canonical loom data type.
        nullable: Whether the column accepts null values.  Defaults to ``True``.

    Example::

        schema = (
            ColumnSchema("order_id", LoomDtype.INT64, nullable=False),
            ColumnSchema("amount",   LoomDtype.FLOAT64),
            ColumnSchema("year",     LoomDtype.INT32, nullable=False),
        )
    """

    name: str
    dtype: LoomDtype
    nullable: bool = True
