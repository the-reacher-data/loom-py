"""DeltaCatalog — TableDiscovery backed by real Delta table metadata.

Reads schema directly from the Delta log via ``deltalake.DeltaTable``.
``update_schema`` is a no-op: Delta maintains its own schema in the log
and ``schema()`` always reflects the current on-disk state.

Storage options are forwarded verbatim to delta-rs — the locator resolves
credentials per table.  See https://delta-io.github.io/delta-rs/api/delta_writer/
"""

from __future__ import annotations

import os

import pyarrow as pa  # type: ignore[import-untyped]
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from loom.etl.schema._schema import ColumnSchema, LoomDtype
from loom.etl.schema._table import TableRef
from loom.etl.storage._locator import TableLocator, _as_locator

# PyArrow string representation → LoomDtype
# Stable because PyArrow's __str__ for primitive types is well-defined.

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


def _arrow_to_loom(arrow_type: pa.DataType) -> LoomDtype:
    """Map a PyArrow DataType to a LoomDtype using the type's string representation."""
    return _ARROW_STR_TO_LOOM.get(str(arrow_type), LoomDtype.NULL)


class DeltaCatalog:
    """Catalog backed by real Delta table metadata.

    Implements :class:`~loom.etl._io.TableDiscovery`.  ``exists()`` and
    ``schema()`` read the Delta log directly via delta-rs — no in-memory
    state is maintained, so the catalog always reflects the current state
    after each write.

    Table existence is detected with ``DeltaTable.is_deltatable(uri)``, which
    works for local paths and cloud URIs alike.

    Args:
        locator: Root URI string, :class:`pathlib.Path`, or any
                 :class:`~loom.etl._locator.TableLocator`.  A plain string or
                 path is wrapped in :class:`~loom.etl._locator.PrefixLocator`
                 automatically.

    Example::

        from loom.etl.backends.polars import DeltaCatalog

        # Simple — plain URI
        catalog = DeltaCatalog("s3://my-lake/")

        # Advanced — explicit locator with credentials
        from loom.etl.storage._locator import PrefixLocator
        catalog = DeltaCatalog(PrefixLocator("s3://my-lake/", storage_options={...}))
    """

    def __init__(self, locator: str | os.PathLike[str] | TableLocator) -> None:
        self._locator = _as_locator(locator)

    def exists(self, ref: TableRef) -> bool:
        """Return ``True`` if the Delta table exists at the resolved URI."""
        loc = self._locator.locate(ref)
        return DeltaTable.is_deltatable(loc.uri, loc.storage_options or None)

    def columns(self, ref: TableRef) -> tuple[str, ...]:
        """Return column names from the Delta log, or ``()`` if the table does not exist."""
        schema = self.schema(ref)
        return tuple(col.name for col in schema) if schema is not None else ()

    def schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None:
        """Return the current schema from the Delta log, or ``None`` if absent.

        Reads ``DeltaTable.schema().to_pyarrow()`` so the result always
        reflects what is actually on disk.

        Args:
            ref: Logical table reference.

        Returns:
            Ordered tuple of :class:`~loom.etl._schema.ColumnSchema`, or
            ``None`` when the table does not yet exist.
        """
        loc = self._locator.locate(ref)
        try:
            dt = DeltaTable(loc.uri, storage_options=loc.storage_options or None)
        except TableNotFoundError:
            return None

        arrow_schema: pa.Schema = dt.schema().to_pyarrow()
        return tuple(
            ColumnSchema(
                name=field.name,
                dtype=_arrow_to_loom(field.type),
                nullable=field.nullable,
            )
            for field in arrow_schema
        )

    def update_schema(self, ref: TableRef, schema: tuple[ColumnSchema, ...]) -> None:
        """No-op — Delta maintains its own schema in the transaction log."""
