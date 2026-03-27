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

Complex / nested type validation
---------------------------------

When a :class:`~loom.etl._schema.ColumnSchema` uses a coarse
:class:`~loom.etl._schema.LoomDtype` (e.g. ``LoomDtype.DATETIME``, or the
structural aliases ``LoomDtype.LIST`` / ``LoomDtype.STRUCT``), validation
accepts any parametrisation of that base type.

When a structural class is used (e.g. :class:`~loom.etl._schema.ListType`,
:class:`~loom.etl._schema.StructType`, :class:`~loom.etl._schema.DatetimeType`),
validation enforces **full structural equality** including inner types at
every nesting level.

EVOLVE + missing complex column
---------------------------------
EVOLVE fills missing columns with ``pl.lit(None).cast(polars_type)``.
For complex columns the Polars type is reconstructed from the
:data:`~loom.etl._schema.LoomType` via
:func:`~loom.etl.backends.polars._dtype.loom_type_to_polars`.
Coarse structural aliases (e.g. ``LoomDtype.LIST``) carry no inner-type
information and therefore cannot produce a valid cast expression —
:exc:`~loom.etl._schema.SchemaError` is raised in that case.

The *missing column* case in EVOLVE adds a typed null expression
(``pl.lit(None).cast(polars_type)``) so the frame always carries
the expected columns in the correct dtype for the writer.

OVERWRITE passes the frame through unchanged — the writer replaces
the table schema with whatever the frame contains.
"""

from __future__ import annotations

import logging

import polars as pl

from loom.etl.backends.polars._dtype import (
    loom_type_to_polars,
    polars_to_loom,
    polars_to_loom_type,
)
from loom.etl.io._target import SchemaMode
from loom.etl.schema._schema import ColumnSchema, LoomDtype, SchemaError, SchemaNotFoundError

_log = logging.getLogger(__name__)

# Re-exported for backwards compatibility — import from loom.etl._schema directly.
__all__ = ["apply_schema", "SchemaNotFoundError", "SchemaError"]


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

    _log.debug("apply_schema mode=%s schema_cols=%d", mode, len(schema))

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

    _log.debug("evolve fill cols=%s", [c.name for c in missing])
    null_exprs = []
    for col in missing:
        try:
            polars_type = loom_type_to_polars(col.dtype)
        except TypeError as exc:
            raise SchemaError(
                f"EVOLVE: cannot fill missing column '{col.name}' (dtype={col.dtype!r}). "
                "Coarse structural aliases (LoomDtype.LIST, LoomDtype.STRUCT, …) carry no "
                "inner-type information. Use the rich LoomType classes (ListType, StructType, …) "
                "or provide the column in the frame."
            ) from exc
        null_exprs.append(pl.lit(None).cast(polars_type).alias(col.name))

    return frame.with_columns(null_exprs)


def _validate_types(
    frame_schema: pl.Schema,
    schema: tuple[ColumnSchema, ...],
    *,
    allow_missing: bool = False,
) -> None:
    """Check that frame column types are compatible with the registered schema.

    Coarse :class:`~loom.etl._schema.LoomDtype` values use a class-level
    comparison (any parametrisation of the base type is accepted).  Rich
    structural types (:class:`~loom.etl._schema.ListType`,
    :class:`~loom.etl._schema.StructType`, :class:`~loom.etl._schema.DatetimeType`,
    etc.) require **full structural equality** at every nesting level.

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

        if isinstance(col.dtype, LoomDtype):
            # Coarse check — class-level mapping, ignores parametrisation.
            actual = polars_to_loom(frame_dtype)
            if actual is not col.dtype:
                raise SchemaError(f"column '{col.name}': expected {col.dtype!r}, got {actual!r}")
        else:
            # Full structural check — exact recursive equality.
            actual_type = polars_to_loom_type(frame_dtype)
            if actual_type != col.dtype:
                raise SchemaError(
                    f"column '{col.name}': expected {col.dtype!r}, got {actual_type!r}"
                )
