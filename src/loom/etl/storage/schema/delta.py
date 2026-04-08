"""delta-rs-backed physical schema reader."""

from __future__ import annotations

import logging

import pyarrow as pa
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from loom.etl.schema._schema import ColumnSchema, LoomDtype
from loom.etl.storage.route.model import CatalogTarget, PathTarget, ResolvedTarget
from loom.etl.storage.schema.model import PhysicalSchema

_log = logging.getLogger(__name__)

_ARROW_STR_TO_LOOM: dict[str, LoomDtype] = {
    "int8": LoomDtype.INT8,
    "int16": LoomDtype.INT16,
    "int32": LoomDtype.INT32,
    "int64": LoomDtype.INT64,
    "uint8": LoomDtype.UINT8,
    "uint16": LoomDtype.UINT16,
    "uint32": LoomDtype.UINT32,
    "uint64": LoomDtype.UINT64,
    "float": LoomDtype.FLOAT32,
    "double": LoomDtype.FLOAT64,
    "string": LoomDtype.UTF8,
    "large_string": LoomDtype.UTF8,
    "utf8": LoomDtype.UTF8,
    "binary": LoomDtype.BINARY,
    "large_binary": LoomDtype.BINARY,
    "bool": LoomDtype.BOOLEAN,
    "date32[day]": LoomDtype.DATE,
    "timestamp[us]": LoomDtype.DATETIME,
    "timestamp[us, tz=UTC]": LoomDtype.DATETIME,
    "duration[us]": LoomDtype.DURATION,
    "time64[us]": LoomDtype.TIME,
    "null": LoomDtype.NULL,
}


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
    arrow_schema: pa.Schema = _to_pyarrow_schema(dt.schema())
    return PhysicalSchema(
        columns=_columns_from_arrow(arrow_schema),
        partition_columns=tuple(dt.metadata().partition_columns),
    )


def _columns_from_arrow(schema: pa.Schema) -> tuple[ColumnSchema, ...]:
    return tuple(
        ColumnSchema(
            name=field.name,
            dtype=_arrow_to_loom(field.type),
            nullable=field.nullable,
        )
        for field in schema
    )


def _arrow_to_loom(arrow_type: pa.DataType) -> LoomDtype:
    dtype = _ARROW_STR_TO_LOOM.get(str(arrow_type))
    if dtype is not None:
        return dtype
    _log.warning(
        "DeltaSchemaReader: unsupported Arrow type %r mapped to LoomDtype.NULL",
        arrow_type,
    )
    return LoomDtype.NULL


def _to_pyarrow_schema(raw_schema: object) -> pa.Schema:
    to_arrow = getattr(raw_schema, "to_arrow", None)
    if not callable(to_arrow):
        raise TypeError(f"Unsupported Delta schema object: {type(raw_schema)!r}")
    # deltalake>=1.5 returns arro3 schema; normalize to PyArrow for stable dtype mapping.
    return pa.schema(to_arrow())
