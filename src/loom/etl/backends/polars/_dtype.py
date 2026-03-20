"""LoomDtype ↔ Polars DataType conversion utilities.

Internal module — not part of the public API.
"""

from __future__ import annotations

import polars as pl

from loom.etl._schema import LoomDtype

# ---------------------------------------------------------------------------
# Mapping tables
# ---------------------------------------------------------------------------

_LOOM_TO_POLARS: dict[LoomDtype, type[pl.DataType]] = {
    LoomDtype.INT8: pl.Int8,
    LoomDtype.INT16: pl.Int16,
    LoomDtype.INT32: pl.Int32,
    LoomDtype.INT64: pl.Int64,
    LoomDtype.UINT8: pl.UInt8,
    LoomDtype.UINT16: pl.UInt16,
    LoomDtype.UINT32: pl.UInt32,
    LoomDtype.UINT64: pl.UInt64,
    LoomDtype.FLOAT32: pl.Float32,
    LoomDtype.FLOAT64: pl.Float64,
    LoomDtype.DECIMAL: pl.Float64,  # best-effort fallback
    LoomDtype.UTF8: pl.String,
    LoomDtype.BINARY: pl.Binary,
    LoomDtype.BOOLEAN: pl.Boolean,
    LoomDtype.DATE: pl.Date,
    LoomDtype.DATETIME: pl.Datetime,
    LoomDtype.DURATION: pl.Duration,
    LoomDtype.TIME: pl.Time,
    LoomDtype.NULL: pl.Null,
}

_POLARS_TO_LOOM: dict[type[pl.DataType], LoomDtype] = {
    pl.Int8: LoomDtype.INT8,
    pl.Int16: LoomDtype.INT16,
    pl.Int32: LoomDtype.INT32,
    pl.Int64: LoomDtype.INT64,
    pl.UInt8: LoomDtype.UINT8,
    pl.UInt16: LoomDtype.UINT16,
    pl.UInt32: LoomDtype.UINT32,
    pl.UInt64: LoomDtype.UINT64,
    pl.Float32: LoomDtype.FLOAT32,
    pl.Float64: LoomDtype.FLOAT64,
    pl.String: LoomDtype.UTF8,
    pl.Binary: LoomDtype.BINARY,
    pl.Boolean: LoomDtype.BOOLEAN,
    pl.Date: LoomDtype.DATE,
    pl.Datetime: LoomDtype.DATETIME,
    pl.Duration: LoomDtype.DURATION,
    pl.Time: LoomDtype.TIME,
    pl.Null: LoomDtype.NULL,
}


def loom_to_polars(dtype: LoomDtype) -> type[pl.DataType]:
    """Return the Polars DataType class for a :class:`~loom.etl._schema.LoomDtype`.

    Args:
        dtype: Canonical Loom dtype.

    Returns:
        Polars DataType class (e.g. ``pl.Int64``).

    Raises:
        KeyError: If *dtype* has no Polars equivalent.
    """
    return _LOOM_TO_POLARS[dtype]


def polars_to_loom(dtype: pl.DataType) -> LoomDtype:
    """Return the :class:`~loom.etl._schema.LoomDtype` for a Polars DataType instance.

    Args:
        dtype: Polars DataType instance (e.g. ``pl.Int64()``).

    Returns:
        Canonical Loom dtype, or :attr:`~loom.etl._schema.LoomDtype.NULL` if
        the Polars type has no direct mapping.
    """
    return _POLARS_TO_LOOM.get(type(dtype), LoomDtype.NULL)
