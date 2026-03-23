"""Unit tests for spark_apply_schema using chispa — no Delta I/O."""

from __future__ import annotations

import pytest
from chispa import assert_df_equality
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from loom.etl import ETLParams, ETLStep, FromTable, IntoTable
from loom.etl._schema import ColumnSchema, LoomDtype, SchemaError, SchemaNotFoundError
from loom.etl._target import SchemaMode
from loom.etl.backends.spark._schema import spark_apply_schema
from loom.etl.testing import ETLScenario

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ID_AMOUNT = (ColumnSchema("id", LoomDtype.INT64), ColumnSchema("amount", LoomDtype.FLOAT64))


# ---------------------------------------------------------------------------
# schema=None
# ---------------------------------------------------------------------------


def test_raises_schema_not_found_when_schema_is_none(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 1.0)], ["id", "amount"])
    with pytest.raises(SchemaNotFoundError):
        spark_apply_schema(frame, None, SchemaMode.STRICT)


def test_raises_schema_not_found_for_evolve_when_schema_is_none(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 1.0)], ["id", "amount"])
    with pytest.raises(SchemaNotFoundError):
        spark_apply_schema(frame, None, SchemaMode.EVOLVE)


def test_overwrite_raises_schema_not_found_when_schema_is_none(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 1.0)], ["id", "amount"])
    with pytest.raises(SchemaNotFoundError):
        spark_apply_schema(frame, None, SchemaMode.OVERWRITE)


# ---------------------------------------------------------------------------
# OVERWRITE — pass-through when schema is provided
# ---------------------------------------------------------------------------


def test_overwrite_returns_frame_unchanged(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 1.0)], ["id", "amount"])
    result = spark_apply_schema(frame, _ID_AMOUNT, SchemaMode.OVERWRITE)
    assert_df_equality(result, frame)


# ---------------------------------------------------------------------------
# STRICT
# ---------------------------------------------------------------------------


def test_strict_exact_match_passes(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 1.0)], ["id", "amount"])
    result = spark_apply_schema(frame, _ID_AMOUNT, SchemaMode.STRICT)
    assert_df_equality(result, frame)


def test_strict_raises_on_extra_column(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 1.0, "x")], ["id", "amount", "extra"])
    with pytest.raises(SchemaError, match="extra"):
        spark_apply_schema(frame, _ID_AMOUNT, SchemaMode.STRICT)


def test_strict_raises_on_missing_column(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1,)], ["id"])
    with pytest.raises(SchemaError, match="amount"):
        spark_apply_schema(frame, _ID_AMOUNT, SchemaMode.STRICT)


def test_strict_raises_on_type_mismatch(spark: SparkSession) -> None:
    # amount as StringType instead of DoubleType
    frame = spark.createDataFrame([(1, "bad")], ["id", "amount"])
    with pytest.raises(SchemaError, match="amount"):
        spark_apply_schema(frame, _ID_AMOUNT, SchemaMode.STRICT)


# ---------------------------------------------------------------------------
# EVOLVE
# ---------------------------------------------------------------------------


def test_evolve_passes_with_matching_columns(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 1.0)], ["id", "amount"])
    result = spark_apply_schema(frame, _ID_AMOUNT, SchemaMode.EVOLVE)
    assert_df_equality(result, frame)


def test_evolve_fills_missing_column_with_typed_null(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1,)], ["id"])
    result = spark_apply_schema(frame, _ID_AMOUNT, SchemaMode.EVOLVE)

    expected = spark.createDataFrame(
        [(1, None)],
        T.StructType(
            [
                T.StructField("id", T.LongType()),
                T.StructField("amount", T.DoubleType()),
            ]
        ),
    )
    assert_df_equality(result, expected)


def test_evolve_preserves_extra_columns(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 1.0, "tag")], ["id", "amount", "tag"])
    result = spark_apply_schema(frame, _ID_AMOUNT, SchemaMode.EVOLVE)
    # extra column survives
    assert "tag" in result.columns


def test_evolve_raises_on_type_mismatch(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, "bad")], ["id", "amount"])
    with pytest.raises(SchemaError, match="amount"):
        spark_apply_schema(frame, _ID_AMOUNT, SchemaMode.EVOLVE)


# ---------------------------------------------------------------------------
# ETLScenario integration — same scenario, different assertions
# ---------------------------------------------------------------------------


ORDERS_SCENARIO = ETLScenario().with_table("raw.orders", [(1, 10.0), (2, 20.0)], ["id", "amount"])


def test_scenario_seeds_frame(step_runner, spark: SparkSession) -> None:  # type: ignore[no-untyped-def]
    """ETLScenario.apply() correctly seeds the runner and executes the step."""

    class NoParams(ETLParams):
        pass

    class DoubleStep(ETLStep[NoParams]):
        orders: FromTable = FromTable("raw.orders")  # type: ignore[assignment]
        target = IntoTable("staging.orders").replace()

        def execute(self, params: NoParams, *, orders: DataFrame) -> DataFrame:  # type: ignore[override]
            return orders.withColumn("amount", F.col("amount") * 2)

    ORDERS_SCENARIO.apply(step_runner)
    step_runner.run(DoubleStep, NoParams())

    expected = spark.createDataFrame([(1, 20.0), (2, 40.0)], ["id", "amount"])
    assert_df_equality(step_runner.result, expected, ignore_row_order=True)
