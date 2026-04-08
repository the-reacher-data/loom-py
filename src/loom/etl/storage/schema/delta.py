"""delta-rs-backed physical schema reader."""

from __future__ import annotations

import polars as pl
import pyarrow as pa
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from loom.etl.storage.route.model import CatalogTarget, PathTarget, ResolvedTarget
from loom.etl.storage.schema.model import PhysicalSchema, PolarsPhysicalSchema


class DeltaSchemaReader:
    """Read physical schema from Delta log for path targets."""

    def read_schema(self, target: ResolvedTarget) -> PhysicalSchema | None:
        """Return physical schema for *target*, or ``None`` if absent."""
        if isinstance(target, CatalogTarget):
            raise ValueError(
                "DeltaSchemaReader only supports path targets; "
                f"got catalog target for {target.logical_ref.ref!r}."
            )
        if not isinstance(target, PathTarget):
            raise TypeError(f"Unsupported resolved target: {type(target)!r}")

        return read_delta_physical_schema(
            target.location.uri,
            target.location.storage_options,
        )


def read_delta_physical_schema(
    uri: str,
    storage_options: dict[str, str] | None = None,
) -> PhysicalSchema | None:
    """Read physical schema directly from a Delta table URI.

    Args:
        uri: Delta table URI/path (including ``uc://catalog.schema.table``).
        storage_options: Optional delta-rs storage options.

    Returns:
        Physical schema when the table exists, else ``None``.
    """
    try:
        dt = DeltaTable(uri, storage_options=storage_options or None)
    except TableNotFoundError:
        return None
    return PolarsPhysicalSchema(
        schema=pl.Schema(_to_pyarrow_schema(dt.schema())),
        partition_columns=tuple(dt.metadata().partition_columns),
    )


def _to_pyarrow_schema(raw_schema: object) -> pa.Schema:
    to_arrow = getattr(raw_schema, "to_arrow", None)
    if not callable(to_arrow):
        raise TypeError(f"Unsupported Delta schema object: {type(raw_schema)!r}")
    # deltalake>=1.5 returns arro3 schema; normalize to PyArrow for stable dtype mapping.
    return pa.schema(to_arrow())
