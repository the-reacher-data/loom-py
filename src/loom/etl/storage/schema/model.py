"""Physical table schema models used by runtime planners."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    import polars as pl
    from pyspark.sql.types import StructType


@dataclass(frozen=True)
class PolarsPhysicalSchema:
    """Physical schema snapshot for one resolved Polars target.

    Args:
        schema: Native Polars schema for write-time alignment.
        partition_columns: Ordered partition columns.
    """

    schema: pl.Schema
    partition_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class SparkPhysicalSchema:
    """Physical schema snapshot for one resolved Spark target.

    Args:
        schema: Native Spark StructType for write-time alignment.
        partition_columns: Ordered partition columns.
    """

    schema: StructType
    partition_columns: tuple[str, ...] = ()


PhysicalSchema: TypeAlias = PolarsPhysicalSchema | SparkPhysicalSchema
