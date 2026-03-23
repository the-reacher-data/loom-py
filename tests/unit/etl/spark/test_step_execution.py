"""Integration tests for ETLExecutor.run_step with PySpark + Delta I/O.

SparkSession is session-scoped (shared) — each test uses a fresh tmp_path
via spark_root so Delta tables never collide between tests.

Requires ``pyspark>=3.5`` and ``delta-spark>=3.2``.  Skipped automatically
when either package is absent (enforced by conftest.py).
"""

from __future__ import annotations

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from loom.etl import ETLParams, ETLStep, FromTable, IntoTable
from loom.etl._schema import LoomDtype, SchemaNotFoundError
from loom.etl._table import TableRef
from loom.etl._target import SchemaMode
from loom.etl.compiler import ETLCompiler
from loom.etl.executor import ETLExecutor
from loom.etl.testing import StubRunObserver

from .conftest import SparkDeltaReader, SparkDeltaWriter, spark_table_path

# ---------------------------------------------------------------------------
# Step definitions
# ---------------------------------------------------------------------------


class NoParams(ETLParams):
    pass


class DoubleAmountStep(ETLStep[NoParams]):
    """Reads raw.orders, doubles the amount, writes staging.orders."""

    orders: FromTable = FromTable("raw.orders")  # type: ignore[assignment]
    target = IntoTable("staging.orders").replace()

    def execute(self, params: NoParams, *, orders: DataFrame) -> DataFrame:  # type: ignore[override]
        return orders.withColumn("amount", F.col("amount") * 2)


class PassThroughStep(ETLStep[NoParams]):
    """Copies raw.events to staging.events unchanged."""

    events: FromTable = FromTable("raw.events")  # type: ignore[assignment]
    target = IntoTable("staging.events").replace()

    def execute(self, params: NoParams, *, events: DataFrame) -> DataFrame:  # type: ignore[override]
        return events


class AppendStep(ETLStep[NoParams]):
    """Appends raw.deltas into staging.ledger."""

    deltas: FromTable = FromTable("raw.deltas")  # type: ignore[assignment]
    target = IntoTable("staging.ledger").append()

    def execute(self, params: NoParams, *, deltas: DataFrame) -> DataFrame:  # type: ignore[override]
        return deltas


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read(spark: SparkSession, root, ref: str) -> DataFrame:
    from pathlib import Path

    path = spark_table_path(Path(root), TableRef(ref))
    return spark.read.format("delta").load(str(path))


# ---------------------------------------------------------------------------
# Happy-path tests
# ---------------------------------------------------------------------------


def test_run_step_writes_transformed_data(
    spark: SparkSession,
    seed_spark_table,
    spark_reader: SparkDeltaReader,
    spark_writer: SparkDeltaWriter,
    spark_root,
) -> None:
    """execute() result lands in the target Delta table."""
    seed_spark_table(
        "raw.orders",
        spark.createDataFrame([(1, 10.0), (2, 20.0), (3, 30.0)], ["id", "amount"]),
    )
    seed_spark_table("staging.orders", spark.createDataFrame([(0, 0.0)], ["id", "amount"]))

    plan = ETLCompiler().compile_step(DoubleAmountStep)
    ETLExecutor(spark_reader, spark_writer).run_step(plan, NoParams())

    result = _read(spark, spark_root, "staging.orders").orderBy("id")
    amounts = [row["amount"] for row in result.collect()]
    assert amounts == pytest.approx([20.0, 40.0, 60.0])


def test_run_step_row_count_preserved(
    spark: SparkSession,
    seed_spark_table,
    spark_reader: SparkDeltaReader,
    spark_writer: SparkDeltaWriter,
    spark_root,
) -> None:
    seed_spark_table(
        "raw.events",
        spark.createDataFrame([(i, i * 10) for i in range(5)], ["ts", "val"]),
    )
    seed_spark_table("staging.events", spark.createDataFrame([(0, 0)], ["ts", "val"]))

    plan = ETLCompiler().compile_step(PassThroughStep)
    ETLExecutor(spark_reader, spark_writer).run_step(plan, NoParams())

    assert _read(spark, spark_root, "staging.events").count() == 5


def test_run_step_replace_overwrites_existing_target(
    spark: SparkSession,
    seed_spark_table,
    spark_reader: SparkDeltaReader,
    spark_writer: SparkDeltaWriter,
    spark_root,
) -> None:
    seed_spark_table("raw.orders", spark.createDataFrame([(1, 5.0)], ["id", "amount"]))
    seed_spark_table("staging.orders", spark.createDataFrame([(99, 999.0)], ["id", "amount"]))

    plan = ETLCompiler().compile_step(DoubleAmountStep)
    ETLExecutor(spark_reader, spark_writer).run_step(plan, NoParams())

    result = _read(spark, spark_root, "staging.orders").collect()
    assert len(result) == 1
    assert result[0]["id"] == 1
    assert result[0]["amount"] == pytest.approx(10.0)


def test_run_step_append_adds_rows(
    spark: SparkSession,
    seed_spark_table,
    spark_reader: SparkDeltaReader,
    spark_writer: SparkDeltaWriter,
    spark_root,
) -> None:
    seed_spark_table("raw.deltas", spark.createDataFrame([(1, 10), (2, 20)], ["id", "v"]))
    seed_spark_table("staging.ledger", spark.createDataFrame([(0, 0)], ["id", "v"]))

    plan = ETLCompiler().compile_step(AppendStep)
    ETLExecutor(spark_reader, spark_writer).run_step(plan, NoParams())

    assert _read(spark, spark_root, "staging.ledger").count() == 3


# ---------------------------------------------------------------------------
# Observer lifecycle
# ---------------------------------------------------------------------------


def test_run_step_emits_success_events(
    spark: SparkSession,
    seed_spark_table,
    spark_reader: SparkDeltaReader,
    spark_writer: SparkDeltaWriter,
) -> None:
    from loom.etl.executor import EventName

    seed_spark_table("raw.orders", spark.createDataFrame([(1, 1.0)], ["id", "amount"]))
    seed_spark_table("staging.orders", spark.createDataFrame([(0, 0.0)], ["id", "amount"]))

    observer = StubRunObserver()
    plan = ETLCompiler().compile_step(DoubleAmountStep)
    ETLExecutor(spark_reader, spark_writer, observers=[observer]).run_step(plan, NoParams())

    assert observer.event_names == [EventName.STEP_START, EventName.STEP_END]
    assert observer.step_statuses == ["success"]


def test_run_step_emits_error_event_on_failure(
    spark_writer: SparkDeltaWriter,
) -> None:
    from loom.etl.executor import RunStatus

    class FailingReader:
        def read(self, spec: object, params: object) -> None:  # type: ignore[override]
            raise RuntimeError("spark read failure")

    observer = StubRunObserver()
    plan = ETLCompiler().compile_step(DoubleAmountStep)

    with pytest.raises(RuntimeError, match="spark read failure"):
        ETLExecutor(FailingReader(), spark_writer, observers=[observer]).run_step(plan, NoParams())

    assert observer.step_statuses == [RunStatus.FAILED]


# ---------------------------------------------------------------------------
# Schema enforcement
# ---------------------------------------------------------------------------


def test_writer_raises_schema_not_found_without_registered_schema(
    spark: SparkSession,
    spark_reader: SparkDeltaReader,
    spark_writer: SparkDeltaWriter,
    seed_spark_table,
) -> None:
    """STRICT write to a table with no registered schema raises SchemaNotFoundError."""
    seed_spark_table("raw.orders", spark.createDataFrame([(1, 1.0)], ["id", "amount"]))
    # staging.orders NOT seeded — catalog has no schema for it

    plan = ETLCompiler().compile_step(DoubleAmountStep)
    with pytest.raises(SchemaNotFoundError):
        ETLExecutor(spark_reader, spark_writer).run_step(plan, NoParams())


def test_writer_overwrite_creates_table_on_first_write(
    spark: SparkSession,
    spark_root,
    spark_catalog,
    seed_spark_table,
    spark_reader: SparkDeltaReader,
    spark_writer: SparkDeltaWriter,
) -> None:
    """OVERWRITE creates the target table when it does not exist yet."""

    seed_spark_table("raw.orders", spark.createDataFrame([(1, 1.0)], ["id", "amount"]))

    class OverwriteStep(ETLStep[NoParams]):
        orders: FromTable = FromTable("raw.orders")  # type: ignore[assignment]
        target = IntoTable("staging.new").replace(schema=SchemaMode.OVERWRITE)

        def execute(self, params: NoParams, *, orders: DataFrame) -> DataFrame:  # type: ignore[override]
            return orders

    plan = ETLCompiler().compile_step(OverwriteStep)
    ETLExecutor(spark_reader, spark_writer).run_step(plan, NoParams())

    assert spark_catalog.exists(TableRef("staging.new"))
    result = _read(spark, spark_root, "staging.new").collect()
    assert len(result) == 1


def test_seed_registers_correct_schema_in_catalog(
    spark: SparkSession,
    seed_spark_table,
    spark_catalog,
) -> None:
    seed_spark_table("raw.orders", spark.createDataFrame([(1, 1.0)], ["id", "amount"]))
    schema = spark_catalog.schema(TableRef("raw.orders"))
    assert schema is not None
    assert schema[0].name == "id"
    assert schema[0].dtype is LoomDtype.INT64
    assert schema[1].name == "amount"
    assert schema[1].dtype is LoomDtype.FLOAT64
