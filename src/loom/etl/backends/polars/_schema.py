"""apply_schema — frame validation and evolution against a registered schema.

Pure function, no Delta or I/O dependency.  Tested independently of the
writer so schema logic can be verified without a real Delta table.

Schema enforcement rules
------------------------

+--------------------------+----------+----------+-----------+
| Situation                | STRICT   | EVOLVE   | OVERWRITE |
+==========================+==========+==========+===========+
| Types match              | OK       | OK       | OK        |
+--------------------------+----------+----------+-----------+
| Type mismatch            | error    | error    | OK        |
+--------------------------+----------+----------+-----------+
| Extra col in frame       | error    | OK → add | OK        |
+--------------------------+----------+----------+-----------+
| Col missing from frame   | error    | OK → nil | OK        |
+--------------------------+----------+----------+-----------+
| schema is None           | error    | error    | error     |
+--------------------------+----------+----------+-----------+

The *missing column* case in EVOLVE adds a typed null expression
(``pl.lit(None).cast(polars_type)``) so the frame always carries
the expected columns in the correct dtype for the writer.

OVERWRITE passes the frame through unchanged — the writer replaces
the table schema with whatever the frame contains.
"""

from __future__ import annotations

import polars as pl

from loom.etl._schema import ColumnSchema
from loom.etl._target import SchemaMode
from loom.etl.backends.polars._dtype import loom_to_polars, polars_to_loom


class SchemaNotFoundError(Exception):
    """Raised when ``apply_schema`` is called but no schema is registered.

    The table schema must be registered in the catalog before any write.
    This prevents silent type inference on first write, which can produce
    incorrect partition definitions, wrong nullability, or unexpected types.

    Register the schema via :meth:`~loom.etl._io.TableDiscovery.update_schema`
    before the first write, or use :class:`~loom.etl.backends.polars.DeltaCatalog`
    with a pre-created Delta table.
    """


class SchemaError(Exception):
    """Raised when the frame is incompatible with the registered schema."""


def apply_schema(
    frame: pl.LazyFrame,
    schema: tuple[ColumnSchema, ...] | None,
    mode: SchemaMode,
) -> pl.LazyFrame:
    """Validate or evolve *frame* against the registered table *schema*.

    Args:
        frame:  Lazy frame produced by the step's ``execute()``.
        schema: Registered table schema from the catalog.  Must not be
                ``None`` — pre-register via ``catalog.update_schema`` before
                the first write.
        mode:   Schema enforcement strategy.

    Returns:
        The original frame (STRICT / EVOLVE compatible case), or the frame
        extended with typed-null columns for any schema columns absent from
        the frame (EVOLVE).  OVERWRITE returns the frame unchanged.

    Raises:
        SchemaNotFoundError: When *schema* is ``None`` for any mode.
        SchemaError:         When the frame violates the schema constraints
                             for the given *mode*.
    """
    if schema is None:
        raise SchemaNotFoundError(
            "No schema registered for this table. "
            "Register the schema via catalog.update_schema() before the first write."
        )

    if mode is SchemaMode.OVERWRITE:
        return frame

    if mode is SchemaMode.STRICT:
        return _apply_strict(frame, schema)
    return _apply_evolve(frame, schema)


# ---------------------------------------------------------------------------
# Internal per-mode helpers
# ---------------------------------------------------------------------------


def _apply_strict(frame: pl.LazyFrame, schema: tuple[ColumnSchema, ...]) -> pl.LazyFrame:
    """Validate frame columns against schema exactly — no extras, no missing."""
    frame_schema = frame.collect_schema()
    schema_names = {col.name for col in schema}
    frame_names = set(frame_schema.names())

    extra = frame_names - schema_names
    if extra:
        raise SchemaError(
            f"STRICT: frame contains columns not in the registered schema: {sorted(extra)}"
        )

    missing = schema_names - frame_names
    if missing:
        raise SchemaError(
            f"STRICT: frame is missing columns required by the registered schema: {sorted(missing)}"
        )

    _validate_types(frame_schema, schema)
    return frame


def _apply_evolve(frame: pl.LazyFrame, schema: tuple[ColumnSchema, ...]) -> pl.LazyFrame:
    """Allow extra columns in the frame; fill missing schema columns with typed nulls."""
    frame_schema = frame.collect_schema()
    frame_names = set(frame_schema.names())

    _validate_types(frame_schema, schema, allow_missing=True)

    missing = [col for col in schema if col.name not in frame_names]
    if not missing:
        return frame

    null_exprs = [pl.lit(None).cast(loom_to_polars(col.dtype)).alias(col.name) for col in missing]
    return frame.with_columns(null_exprs)


def _validate_types(
    frame_schema: pl.Schema,
    schema: tuple[ColumnSchema, ...],
    *,
    allow_missing: bool = False,
) -> None:
    """Check that frame column types are compatible with the registered schema.

    Args:
        frame_schema:  Polars schema of the frame.
        schema:        Registered Loom schema.
        allow_missing: When ``True``, columns absent from the frame are skipped
                       (used by EVOLVE to let the null-fill step handle them).

    Raises:
        SchemaError: On any type mismatch between frame and schema.
    """
    for col in schema:
        frame_dtype = frame_schema.get(col.name)
        if frame_dtype is None:
            if not allow_missing:
                raise SchemaError(f"column '{col.name}' is missing from the frame")
            continue

        actual_loom = polars_to_loom(frame_dtype)
        if actual_loom is not col.dtype:
            raise SchemaError(f"column '{col.name}': expected {col.dtype!r}, got {actual_loom!r}")
