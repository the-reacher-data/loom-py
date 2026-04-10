"""Spark physical schema types and schema alignment.

StructType — field-by-field casting
-------------------------------------
For :class:`~pyspark.sql.types.StructType` columns the cast uses Spark dot
notation (``F.col("parent.field")``) to access each nested field, then
reconstructs the struct with ``F.struct([...])``.  This correctly handles
arbitrarily nested structures such as ``Struct[Struct[...]]`` at any depth.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from loom.etl.declarative.target import SchemaMode
from loom.etl.schema._schema import SchemaNotFoundError

_log = logging.getLogger(__name__)

__all__ = ["apply_schema_spark", "SparkPhysicalSchema", "SchemaNotFoundError"]


@dataclass(frozen=True)
class SparkPhysicalSchema:
    """Physical schema snapshot for a resolved Spark target.

    Args:
        schema: Native Spark StructType for write-time alignment.
        partition_columns: Ordered partition columns.
    """

    schema: T.StructType
    partition_columns: tuple[str, ...] = ()


def apply_schema_spark(
    frame: DataFrame,
    schema: T.StructType | None,
    mode: SchemaMode,
) -> DataFrame:
    """Conform *frame* to the destination table's *schema* according to *mode*.

    Args:
        frame:  Spark DataFrame produced by the step's ``execute()``.
        schema: Destination table's native PySpark StructType schema.  May be
                ``None`` only when *mode* is ``OVERWRITE``.
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

    frame_cols = set(frame.columns)
    present = [f for f in schema.fields if f.name in frame_cols]
    missing = [f for f in schema.fields if f.name not in frame_cols]

    _log.debug(
        "spark_apply_schema mode=%s present=%d missing=%d",
        mode,
        len(present),
        len(missing),
    )

    for field in present:
        frame = frame.withColumn(field.name, _cast_col(field.name, field.dataType))

    for field in missing:
        frame = frame.withColumn(field.name, _null_col(field.dataType))

    if mode is SchemaMode.STRICT:
        frame = frame.select([f.name for f in schema.fields])

    return frame


def _cast_col(col_ref: str, dtype: T.DataType) -> Column:
    """Recursively cast the column at *col_ref* to *dtype*."""
    if isinstance(dtype, T.StructType):
        field_exprs = [
            _cast_col(f"{col_ref}.{f.name}", f.dataType).alias(f.name) for f in dtype.fields
        ]
        return F.struct(field_exprs)
    return F.col(col_ref).cast(dtype)


def _null_col(dtype: T.DataType) -> Column:
    """Return a typed-null Spark Column expression for *dtype*."""
    return F.lit(None).cast(dtype)
