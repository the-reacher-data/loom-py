"""Schema evolution mode enum for TABLE targets."""

from __future__ import annotations

from enum import StrEnum


class SchemaMode(StrEnum):
    """Schema evolution strategy applied by the target writer before each write.

    Values:

    * ``STRICT``    — fail on incompatible schema changes.
    * ``EVOLVE``    — allow additive evolution where supported.
    * ``OVERWRITE`` — replace table schema with incoming schema.
    """

    STRICT = "strict"
    EVOLVE = "evolve"
    OVERWRITE = "overwrite"


__all__ = ["SchemaMode"]
