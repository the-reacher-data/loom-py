"""Schema-read contracts and models for ETL storage runtime."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, TypeAlias

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


class SchemaReader(Protocol):
    """Read physical schema for a resolved table target."""

    def read_schema(self, target: Any) -> PhysicalSchema | None:
        """Return physical schema, or ``None`` when table does not exist."""
        ...


__all__ = ["PhysicalSchema", "PolarsPhysicalSchema", "SparkPhysicalSchema", "SchemaReader"]
