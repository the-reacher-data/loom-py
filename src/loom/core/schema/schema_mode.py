"""Schema evolution mode shared by ETL and streaming layers."""

from __future__ import annotations

from enum import StrEnum


class SchemaMode(StrEnum):
    """Schema evolution strategy applied before each write to a storage target.

    Values:
        STRICT:    Fail on incompatible schema changes.
        EVOLVE:    Allow additive evolution where the backend supports it.
        OVERWRITE: Replace the table schema with the incoming schema.
    """

    STRICT = "strict"
    EVOLVE = "evolve"
    OVERWRITE = "overwrite"


__all__ = ["SchemaMode"]
