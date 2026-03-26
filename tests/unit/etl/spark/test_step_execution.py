"""Integration tests for ETLExecutor.run_step with PySpark + Delta I/O."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from loom.etl import ETLParams, ETLStep, FromTable, IntoTable
from loom.etl._format import Format
from loom.etl._schema import LoomDtype, SchemaNotFoundError
from loom.etl._source import SourceKind, SourceSpec
from loom.etl._table import TableRef
from loom.etl._target import SchemaMode, TargetSpec, WriteMode
from loom.etl.compiler import ETLCompiler
from loom.etl.executor import ETLExecutor, EventName, RunStatus
from loom.etl.testing import StubRunObserver

from .conftest import SparkDeltaReader, SparkDeltaWriter, spark_table_path


class NoParams(ETLParams):
    pass


class DoubleAmountStep(ETLStep[NoParams]):
    orders: FromTable = FromTable("raw.orders")  # type: ignore[assignment]
    target = IntoTable("staging.orders").replace()

    def execute(self, params: NoParams, *, orders: DataFrame) -> DataFrame:  # type: ignore[override]
        return orders.withColumn("amount", F.col("amount") * 2)


class PassThroughStep(ETLStep[NoParams]):
    events: FromTable = FromTable("raw.events")  # type: ignore[assignment]
    target = IntoTable("staging.events").replace()

    def execute(self, params: NoParams, *, events: DataFrame) -> DataFrame:  # type: ignore[override]
        return events


class AppendStep(ETLStep[NoParams]):
    deltas: FromTable = FromTable("raw.deltas")  # type: ignore[assignment]
    target = IntoTable("staging.ledger").append()

    def execute(self, params: NoParams, *, deltas: DataFrame) -> DataFrame:  # type: ignore[override]
        return deltas


SeedSparkTable = Callable[[str, DataFrame], None]


@dataclass(frozen=True)
class CountScenario:
    step_type: type[ETLStep[NoParams]]
    source_table: str
    source_rows: list[tuple[object, ...]]
    source_columns: list[str]
    target_table: str
    target_rows: list[tuple[object, ...]]
    target_columns: list[str]
    expected_count: int


def _read(spark: SparkSession, root: Path, ref: str) -> DataFrame:
    path = spark_table_path(root, TableRef(ref))
    return spark.read.format("delta").load(str(path))


def _seed(
    spark: SparkSession,
    seed_spark_table: SeedSparkTable,
    table_name: str,
    rows: list[tuple[object, ...]],
    columns: list[str],
) -> None:
    seed_spark_table(table_name, spark.createDataFrame(rows, columns))


class TestRunStepDataFlow:
    _COUNT_SCENARIOS = (
        CountScenario(
            step_type=PassThroughStep,
            source_table="raw.events",
            source_rows=[(i, i * 10) for i in range(5)],
            source_columns=["ts", "val"],
            target_table="staging.events",
            target_rows=[(0, 0)],
            target_columns=["ts", "val"],
            expected_count=5,
        ),
        CountScenario(
            step_type=AppendStep,
            source_table="raw.deltas",
            source_rows=[(1, 10), (2, 20)],
            source_columns=["id", "v"],
            target_table="staging.ledger",
            target_rows=[(0, 0)],
            target_columns=["id", "v"],
            expected_count=3,
        ),
    )

    @pytest.mark.parametrize("scenario", _COUNT_SCENARIOS)
    def test_run_step_writes_expected_row_count(
        self,
        scenario: CountScenario,
        spark: SparkSession,
        seed_spark_table: SeedSparkTable,
        spark_reader: SparkDeltaReader,
        spark_writer: SparkDeltaWriter,
        spark_root: Path,
    ) -> None:
        _seed(
            spark,
            seed_spark_table,
            scenario.source_table,
            scenario.source_rows,
            scenario.source_columns,
        )
        _seed(
            spark,
            seed_spark_table,
            scenario.target_table,
            scenario.target_rows,
            scenario.target_columns,
        )

        plan = ETLCompiler().compile_step(scenario.step_type)
        ETLExecutor(spark_reader, spark_writer).run_step(plan, NoParams())

        assert _read(spark, spark_root, scenario.target_table).count() == scenario.expected_count

    def test_run_step_replace_overwrites_and_applies_transform(
        self,
        spark: SparkSession,
        seed_spark_table: SeedSparkTable,
        spark_reader: SparkDeltaReader,
        spark_writer: SparkDeltaWriter,
        spark_root: Path,
    ) -> None:
        _seed(
            spark,
            seed_spark_table,
            "raw.orders",
            [(1, 10.0), (2, 20.0), (3, 30.0)],
            ["id", "amount"],
        )
        _seed(
            spark,
            seed_spark_table,
            "staging.orders",
            [(99, 999.0)],
            ["id", "amount"],
        )

        plan = ETLCompiler().compile_step(DoubleAmountStep)
        ETLExecutor(spark_reader, spark_writer).run_step(plan, NoParams())

        rows = _read(spark, spark_root, "staging.orders").orderBy("id").collect()
        assert [row["id"] for row in rows] == [1, 2, 3]
        assert [row["amount"] for row in rows] == pytest.approx([20.0, 40.0, 60.0])


class TestRunStepEvents:
    def test_run_step_emits_success_events(
        self,
        spark: SparkSession,
        seed_spark_table: SeedSparkTable,
        spark_reader: SparkDeltaReader,
        spark_writer: SparkDeltaWriter,
    ) -> None:
        _seed(spark, seed_spark_table, "raw.orders", [(1, 1.0)], ["id", "amount"])
        _seed(spark, seed_spark_table, "staging.orders", [(0, 0.0)], ["id", "amount"])

        observer = StubRunObserver()
        plan = ETLCompiler().compile_step(DoubleAmountStep)
        ETLExecutor(spark_reader, spark_writer, observers=[observer]).run_step(plan, NoParams())

        assert observer.event_names == [EventName.STEP_START, EventName.STEP_END]
        assert observer.step_statuses == [RunStatus.SUCCESS]

    def test_run_step_emits_error_event_on_failure(self, spark_writer: SparkDeltaWriter) -> None:
        class FailingReader:
            def read(self, spec: object, params: object) -> DataFrame:  # type: ignore[override]
                raise RuntimeError("spark read failure")

        observer = StubRunObserver()
        plan = ETLCompiler().compile_step(DoubleAmountStep)

        with pytest.raises(RuntimeError, match="spark read failure"):
            ETLExecutor(FailingReader(), spark_writer, observers=[observer]).run_step(
                plan, NoParams()
            )

        assert observer.step_statuses == [RunStatus.FAILED]


class TestRunStepContracts:
    def test_writer_raises_schema_not_found_without_registered_schema(
        self,
        spark: SparkSession,
        seed_spark_table: SeedSparkTable,
        spark_reader: SparkDeltaReader,
        spark_writer: SparkDeltaWriter,
    ) -> None:
        _seed(spark, seed_spark_table, "raw.orders", [(1, 1.0)], ["id", "amount"])

        plan = ETLCompiler().compile_step(DoubleAmountStep)
        with pytest.raises(SchemaNotFoundError):
            ETLExecutor(spark_reader, spark_writer).run_step(plan, NoParams())

    def test_writer_overwrite_creates_table_on_first_write(
        self,
        spark: SparkSession,
        spark_root: Path,
        spark_catalog,
        seed_spark_table: SeedSparkTable,
        spark_reader: SparkDeltaReader,
        spark_writer: SparkDeltaWriter,
    ) -> None:
        _seed(spark, seed_spark_table, "raw.orders", [(1, 1.0)], ["id", "amount"])

        class OverwriteStep(ETLStep[NoParams]):
            orders: FromTable = FromTable("raw.orders")  # type: ignore[assignment]
            target = IntoTable("staging.new").replace(schema=SchemaMode.OVERWRITE)

            def execute(self, params: NoParams, *, orders: DataFrame) -> DataFrame:  # type: ignore[override]
                return orders

        plan = ETLCompiler().compile_step(OverwriteStep)
        ETLExecutor(spark_reader, spark_writer).run_step(plan, NoParams())

        assert spark_catalog.exists(TableRef("staging.new"))
        assert _read(spark, spark_root, "staging.new").count() == 1

    def test_seed_registers_correct_schema_in_catalog(
        self,
        spark: SparkSession,
        seed_spark_table: SeedSparkTable,
        spark_catalog,
    ) -> None:
        _seed(spark, seed_spark_table, "raw.orders", [(1, 1.0)], ["id", "amount"])

        schema = spark_catalog.schema(TableRef("raw.orders"))
        assert schema is not None
        assert schema[0].name == "id"
        assert schema[0].dtype is LoomDtype.INT64
        assert schema[1].name == "amount"
        assert schema[1].dtype is LoomDtype.FLOAT64


class TestSparkReaderWriterTypeGuards:
    @pytest.mark.parametrize("role", ["reader", "writer"])
    def test_delta_components_reject_file_specs(
        self,
        role: str,
        spark: SparkSession,
        spark_reader: SparkDeltaReader,
        spark_writer: SparkDeltaWriter,
    ) -> None:
        if role == "reader":
            spec = SourceSpec(
                alias="data",
                kind=SourceKind.FILE,
                format=Format.CSV,
                path="s3://bucket/data.csv",
            )
            with pytest.raises(TypeError, match="FILE"):
                spark_reader.read(spec, None)
            return

        spec = TargetSpec(mode=WriteMode.REPLACE, format=Format.CSV, path="s3://bucket/out.csv")
        frame = spark.createDataFrame([(1,)], ["id"])
        with pytest.raises(TypeError, match="FILE"):
            spark_writer.write(frame, spec, None)
