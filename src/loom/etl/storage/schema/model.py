"""Physical table schema model used by runtime planners."""

from __future__ import annotations

from dataclasses import dataclass

from loom.etl.schema._schema import ColumnSchema


@dataclass(frozen=True)
class PhysicalSchema:
    """Physical schema snapshot for one resolved table.

    Args:
        columns: Ordered physical columns.
        partition_columns: Ordered partition columns.
    """

    columns: tuple[ColumnSchema, ...]
    partition_columns: tuple[str, ...] = ()
