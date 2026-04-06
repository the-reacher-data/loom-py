"""Integration tests for ETLExecutor.run_step with PySpark + Delta I/O."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from loom.etl import ETLParams, ETLStep, Format, FromFile, FromTable, IntoFile, IntoTable
from loom.etl.compiler import ETLCompiler
from loom.etl.executor import ETLExecutor, EventName, RunStatus
from loom.etl.io._read_options import CsvReadOptions
from loom.etl.io.source import FileSourceSpec
from loom.etl.io.target import SchemaMode
from loom.etl.io.target._file import FileSpec
from loom.etl.io.target._table import ReplacePartitionsSpec
from loom.etl.schema._schema import ColumnSchema, LoomDtype, SchemaNotFoundError
from loom.etl.schema._table import TableRef
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


class _PayloadContract:
    store: str
    amount: float
    items: int


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


def test_writer_replace_partitions_first_run_creates_partitioned_table(
    spark: SparkSession,
    spark_writer: SparkDeltaWriter,
    spark_root: Path,
) -> None:
    spec = ReplacePartitionsSpec(
        table_ref=TableRef("staging.partitioned"),
        partition_cols=("year",),
        schema_mode=SchemaMode.OVERWRITE,
    )
    frame = spark.createDataFrame([(2024, 99.0)], ["year", "v"])
    spark_writer.write(frame, spec, None)

    path = spark_table_path(spark_root, TableRef("staging.partitioned"))
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").collect()[0].asDict()
    assert detail["partitionColumns"] == ["year"]


def test_writer_append_creates_table_on_first_write(
    spark: SparkSession,
    spark_writer: SparkDeltaWriter,
    spark_root: Path,
) -> None:
    frame = spark.createDataFrame([(1, 10.0)], ["id", "v"])
    spark_writer.append(frame, TableRef("staging.append_first"), None)

    path = spark_table_path(spark_root, TableRef("staging.append_first"))
    assert spark.read.format("delta").load(str(path)).count() == 1


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

    def test_run_step_supports_csv_reporting_target(
        self,
        spark: SparkSession,
        seed_spark_table: SeedSparkTable,
        spark_reader: SparkDeltaReader,
        spark_writer: SparkDeltaWriter,
        tmp_path: Path,
    ) -> None:
        report_path = tmp_path / "report.csv"
        _seed(
            spark,
            seed_spark_table,
            "raw.orders",
            [(1, 10.0), (2, 20.0)],
            ["id", "amount"],
        )

        class ReportStep(ETLStep[NoParams]):
            orders: FromTable = FromTable("raw.orders")  # type: ignore[assignment]
            target = IntoFile(str(report_path), format=Format.CSV)

            def execute(self, params: NoParams, *, orders: DataFrame) -> DataFrame:  # type: ignore[override]
                _ = params
                return orders.select("id", "amount")

        plan = ETLCompiler().compile_step(ReportStep)
        ETLExecutor(spark_reader, spark_writer).run_step(plan, NoParams())

        out = spark.read.option("header", "true").csv(str(report_path)).orderBy("id").collect()
        assert [row["id"] for row in out] == ["1", "2"]
        assert [float(row["amount"]) for row in out] == pytest.approx([10.0, 20.0])


class TestSparkReaderWriterTypeGuards:
    def test_legacy_delta_components_reject_file_specs(
        self,
        spark: SparkSession,
    ) -> None:
        from loom.etl.backends.spark._reader import SparkDeltaReader as SparkDeltaTableReader
        from loom.etl.backends.spark._writer import SparkDeltaWriter as SparkDeltaTableWriter

        reader = SparkDeltaTableReader(spark, None)
        writer = SparkDeltaTableWriter(spark, None)

        # With typed specs, the legacy reader only accepts TableSourceSpec.
        # Passing a FileSourceSpec (a runtime contract violation) raises AttributeError.
        read_spec = FileSourceSpec(alias="data", path="s3://bucket/data.csv", format=Format.CSV)
        with pytest.raises((TypeError, AttributeError)):
            reader.read(read_spec, None)  # type: ignore[arg-type]

        write_spec = FileSpec(path="s3://bucket/out.csv", format=Format.CSV)
        frame = spark.createDataFrame([(1,)], ["id"])
        with pytest.raises(TypeError, match="TABLE targets"):
            writer.write(frame, write_spec, None)

    def test_unified_components_support_file_specs(
        self,
        spark: SparkSession,
        tmp_path: Path,
        spark_reader: SparkDeltaReader,
        spark_writer: SparkDeltaWriter,
    ) -> None:
        in_path = tmp_path / "in.csv"
        in_path.write_text("id;amount\n1;10.0\n2;20.0\n", encoding="utf-8")

        read_spec = FileSourceSpec(
            alias="data",
            path=str(in_path),
            format=Format.CSV,
            read_options=CsvReadOptions(separator=";"),
            schema=(ColumnSchema("amount", LoomDtype.FLOAT64),),
        )
        frame = spark_reader.read(read_spec, None)
        assert frame.count() == 2

        out_spec = FileSpec(path=str(tmp_path / "out.csv"), format=Format.CSV)
        spark_writer.write(frame, out_spec, None)

        out = spark.read.option("header", "true").csv(out_spec.path)
        assert out.count() == 2

    def test_unified_reader_rejects_unsupported_csv_skip_rows(
        self,
        spark_reader: SparkDeltaReader,
        tmp_path: Path,
    ) -> None:
        read_spec = FileSourceSpec(
            alias="data",
            path=str(tmp_path / "data.csv"),
            format=Format.CSV,
            read_options=CsvReadOptions(skip_rows=1),
        )
        with pytest.raises(ValueError, match="skip_rows"):
            spark_reader.read(read_spec, None)

    def test_unified_reader_parses_json_string_column_from_json_file(
        self,
        spark_reader: SparkDeltaReader,
        tmp_path: Path,
    ) -> None:
        source_path = tmp_path / "events.json"
        source_path.write_text(
            '{"event_id":1,"payload":"{\\"store\\":\\"es\\",\\"amount\\":10.0,\\"items\\":2}"}\n'
            '{"event_id":2,"payload":"{\\"store\\":\\"uk\\",\\"amount\\":7.5,\\"items\\":1}"}\n',
            encoding="utf-8",
        )

        spec = (
            FromFile(str(source_path), format=Format.JSON)
            .parse_json("payload", _PayloadContract)
            ._to_spec("events")
        )
        result = spark_reader.read(spec, None).orderBy("event_id").collect()

        assert [row["payload"]["store"] for row in result] == ["es", "uk"]
        assert [row["payload"]["items"] for row in result] == [2, 1]
        assert [row["payload"]["amount"] for row in result] == pytest.approx([10.0, 7.5])

    def test_unified_components_reject_xlsx_without_plugin(
        self,
        spark: SparkSession,
        spark_reader: SparkDeltaReader,
        spark_writer: SparkDeltaWriter,
        tmp_path: Path,
    ) -> None:
        input_path = tmp_path / "data.xlsx"
        output_path = tmp_path / "out.xlsx"
        read_spec = FileSourceSpec(alias="data", path=str(input_path), format=Format.XLSX)
        with pytest.raises(TypeError, match="XLSX"):
            spark_reader.read(read_spec, None)

        write_spec = FileSpec(path=str(output_path), format=Format.XLSX)
        frame = spark.createDataFrame([(1,)], ["id"])
        with pytest.raises(TypeError, match="XLSX"):
            spark_writer.write(frame, write_spec, None)
