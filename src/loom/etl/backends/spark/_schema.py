"""spark_apply_schema — frame validation and evolution for PySpark DataFrames.

Same STRICT / EVOLVE / OVERWRITE semantics as the Polars backend but
operating on ``pyspark.sql.DataFrame`` instead of ``pl.LazyFrame``.

See :mod:`loom.etl.backends.polars._schema` for the full rules table.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from loom.etl.backends.spark._dtype import loom_type_to_spark, spark_to_loom
from loom.etl.io._target import SchemaMode
from loom.etl.schema._schema import ColumnSchema, SchemaError, SchemaNotFoundError


def spark_apply_schema(
    frame: DataFrame,
    schema: tuple[ColumnSchema, ...] | None,
    mode: SchemaMode,
) -> DataFrame:
    """Validate or evolve *frame* against the registered table *schema*.

    Args:
        frame:  Spark DataFrame produced by the step's ``execute()``.
        schema: Registered table schema from the catalog.  Must not be
                ``None`` — pre-register via ``catalog.update_schema`` before
                the first write.
        mode:   Schema enforcement strategy.

    Returns:
        The original frame (STRICT / compatible EVOLVE), or the frame
        extended with typed-null columns for schema columns absent from
        the frame (EVOLVE).  OVERWRITE returns the frame unchanged.

    Raises:
        SchemaNotFoundError: When *schema* is ``None`` for any mode.
        SchemaError:         When the frame violates schema constraints.
    """
    if schema is None:
        raise SchemaNotFoundError(
            "No schema registered for this table. "
            "Register the schema via catalog.update_schema() before the first write."
        )

    if mode is SchemaMode.OVERWRITE:
        return frame

    if mode is SchemaMode.STRICT:
        return _strict(frame, schema)
    return _evolve(frame, schema)


# ---------------------------------------------------------------------------
# Internal per-mode helpers
# ---------------------------------------------------------------------------


def _strict(frame: DataFrame, schema: tuple[ColumnSchema, ...]) -> DataFrame:
    schema_names = {col.name for col in schema}
    frame_names = set(frame.columns)

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

    _validate_types(frame, schema)
    return frame


def _evolve(frame: DataFrame, schema: tuple[ColumnSchema, ...]) -> DataFrame:
    frame_names = set(frame.columns)
    _validate_types(frame, schema, allow_missing=True)

    missing = [col for col in schema if col.name not in frame_names]
    if not missing:
        return frame

    null_cols = [F.lit(None).cast(loom_type_to_spark(col.dtype)).alias(col.name) for col in missing]
    return frame.withColumns({col.name: expr for col, expr in zip(missing, null_cols, strict=True)})


def _validate_types(
    frame: DataFrame,
    schema: tuple[ColumnSchema, ...],
    *,
    allow_missing: bool = False,
) -> None:
    frame_schema = {f.name: f.dataType for f in frame.schema.fields}

    for col in schema:
        spark_dtype = frame_schema.get(col.name)
        if spark_dtype is None:
            if not allow_missing:
                raise SchemaError(f"column '{col.name}' is missing from the frame")
            continue

        actual_loom = spark_to_loom(spark_dtype)
        if actual_loom is not col.dtype:
            raise SchemaError(f"column '{col.name}': expected {col.dtype!r}, got {actual_loom!r}")
