"""Schema reader contract for runtime write planning."""

from __future__ import annotations

from typing import Protocol

from loom.etl.storage.route.model import ResolvedTarget
from loom.etl.storage.schema.model import PhysicalSchema


class SchemaReader(Protocol):
    """Read physical schema for a resolved table target."""

    def read_schema(self, target: ResolvedTarget) -> PhysicalSchema | None:
        """Return physical schema, or ``None`` when table does not exist."""
        ...
