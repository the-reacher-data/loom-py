"""Unit tests for spark_apply_schema using chispa — no Delta I/O."""

from __future__ import annotations

import pytest
from chispa import assert_df_equality
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from loom.etl import ETLParams, ETLStep, FromTable, IntoTable
from loom.etl.backends.spark._schema import spark_apply_schema
from loom.etl.io._target import SchemaMode
from loom.etl.schema._schema import ColumnSchema, LoomDtype, SchemaError, SchemaNotFoundError
from loom.etl.testing import ETLScenario

_ID_AMOUNT = (ColumnSchema("id", LoomDtype.INT64), ColumnSchema("amount", LoomDtype.FLOAT64))


@pytest.mark.parametrize("mode", list(SchemaMode))
def test_raises_schema_not_found_when_schema_is_none(mode: SchemaMode, spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 1.0)], ["id", "amount"])
    with pytest.raises(SchemaNotFoundError):
        spark_apply_schema(frame, None, mode)


@pytest.mark.parametrize("mode", [SchemaMode.OVERWRITE, SchemaMode.STRICT, SchemaMode.EVOLVE])
def test_apply_schema_passes_with_matching_frame(mode: SchemaMode, spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 1.0)], ["id", "amount"])
    result = spark_apply_schema(frame, _ID_AMOUNT, mode)
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
    frame = spark.createDataFrame([(1, "bad")], ["id", "amount"])
    with pytest.raises(SchemaError, match="amount"):
        spark_apply_schema(frame, _ID_AMOUNT, SchemaMode.STRICT)


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
    assert "tag" in result.columns


def test_evolve_raises_on_type_mismatch(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, "bad")], ["id", "amount"])
    with pytest.raises(SchemaError, match="amount"):
        spark_apply_schema(frame, _ID_AMOUNT, SchemaMode.EVOLVE)


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
    result = step_runner.run(DoubleStep, NoParams())

    expected = spark.createDataFrame([(1, 20.0), (2, 40.0)], ["id", "amount"])
    expected_rows = [tuple(row) for row in expected.orderBy("id").collect()]
    actual_rows = list(result.to_polars().sort("id").iter_rows())
    assert actual_rows == expected_rows
