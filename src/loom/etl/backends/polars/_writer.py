"""PolarsDeltaWriter — TargetWriter backed by Polars + delta-rs.

Enforces schema validation/evolution via ``apply_schema`` before each write.
The catalog is used to look up the registered schema and is updated after
a successful write so subsequent steps see the current state.

OVERWRITE mode is the sole exception to the "schema must be registered" rule:
the table and its schema are created from the frame on first write, which
is safe because the user explicitly requested a full schema replacement.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import polars as pl
from deltalake import write_deltalake

from loom.etl._io import TableDiscovery
from loom.etl._schema import ColumnSchema
from loom.etl._table import TableRef
from loom.etl._target import SchemaMode, TargetSpec, WriteMode
from loom.etl.backends.polars._dtype import polars_to_loom
from loom.etl.backends.polars._schema import apply_schema


class PolarsDeltaWriter:
    """Write ETL step results to Delta tables using Polars + delta-rs.

    Implements :class:`~loom.etl._io.TargetWriter`.

    Validates or evolves the frame schema via :func:`~loom.etl.backends.polars._schema.apply_schema`
    before each write, then updates the catalog so later steps see the evolved
    schema.

    OVERWRITE mode is the only mode allowed to create a new table from scratch
    — the caller explicitly accepts whatever schema the frame carries.

    Args:
        root:    Filesystem root.  Table paths are resolved as
                 ``root/<schema>/<table>/``.
        catalog: Catalog used for schema lookup and post-write update.

    Example::

        writer = PolarsDeltaWriter(Path("/data/delta"), catalog)
        writer.write(frame, spec, params)
    """

    def __init__(self, root: Path, catalog: TableDiscovery) -> None:
        self._root = root
        self._catalog = catalog

    def write(self, frame: pl.LazyFrame, spec: TargetSpec, params_instance: Any) -> None:
        """Validate schema, collect frame, and write it to the Delta target.

        Args:
            frame:           Lazy frame produced by the step's ``execute()``.
            spec:            Compiled target spec (mode, table_ref, schema_mode, …).
            params_instance: Concrete params (unused currently; reserved for
                             partition-value resolution in a future sprint).

        Raises:
            AssertionError:    If *spec* is a FILE target (unsupported here).
            SchemaNotFoundError: When the table has no registered schema and
                                 mode is not OVERWRITE.
            SchemaError:       When the frame violates the registered schema.
        """
        assert spec.table_ref is not None, (
            f"PolarsDeltaWriter only supports TABLE targets; got FILE spec: {spec}"
        )

        table_ref = spec.table_ref
        existing_schema = self._catalog.schema(table_ref)

        # OVERWRITE on a non-existent table: skip apply_schema, create the table.
        if existing_schema is None and spec.schema_mode is SchemaMode.OVERWRITE:
            self._write_frame(frame.collect(), spec)
            self._register_schema(table_ref, frame)
            return

        validated = apply_schema(frame, existing_schema, spec.schema_mode)
        self._write_frame(validated.collect(), spec)
        self._register_schema(table_ref, validated)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _write_frame(self, df: pl.DataFrame, spec: TargetSpec) -> None:
        path = self._table_path(spec.table_ref)  # type: ignore[arg-type]
        path.mkdir(parents=True, exist_ok=True)
        delta_mode = _write_mode(spec.mode)
        write_deltalake(str(path), df.to_arrow(), mode=delta_mode)

    def _register_schema(self, ref: TableRef, frame: pl.LazyFrame) -> None:
        """Update the catalog schema from the frame's collect_schema()."""
        polars_schema = frame.collect_schema()
        schema = tuple(
            ColumnSchema(name=name, dtype=polars_to_loom(dtype))
            for name, dtype in polars_schema.items()
        )
        self._catalog.update_schema(ref, schema)

    def _table_path(self, ref: TableRef) -> Path:
        return self._root.joinpath(*ref.ref.split("."))


def _write_mode(mode: WriteMode) -> Literal["overwrite", "append"]:
    if mode is WriteMode.APPEND:
        return "append"
    return "overwrite"
