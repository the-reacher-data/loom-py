"""Normal runtime contract tests for ETL definition modules."""

from __future__ import annotations

import importlib
from datetime import UTC, datetime
from types import ModuleType

import pytest


def _reload_module(name: str) -> ModuleType:
    return importlib.reload(importlib.import_module(name))


def _reload_modules(*names: str) -> dict[str, ModuleType]:
    return {name: _reload_module(name) for name in names}


def test_definition_modules_support_reload_contracts() -> None:
    modules = _reload_modules(
        "loom.etl.io._format",
        "loom.etl.io._read_options",
        "loom.etl.io._write_options",
        "loom.etl.io.target._file",
        "loom.etl.io.target._table",
        "loom.etl.io.target._temp",
        "loom.etl.schema._schema",
    )

    assert modules["loom.etl.io._format"].Format.CSV.value == "csv"
    assert modules["loom.etl.io._read_options"].CsvReadOptions().separator == ","
    assert modules["loom.etl.io._write_options"].ParquetWriteOptions().compression == "zstd"
    assert modules["loom.etl.io.target._file"].FileSpec is not None
    assert modules["loom.etl.io.target._table"].AppendSpec is not None
    assert modules["loom.etl.io.target._temp"].TempSpec is not None
    assert modules["loom.etl.schema._schema"].ColumnSchema is not None


@pytest.mark.parametrize(
    ("module_name", "exported_symbol"),
    [
        ("loom.etl", "FromTable"),
        ("loom.etl.compiler", "ETLCompiler"),
        ("loom.etl.executor", "ExecutionRecordsObserver"),
        ("loom.etl.io", "IntoTable"),
        ("loom.etl.pipeline", "ETLStep"),
        ("loom.etl.runner", "ETLRunner"),
        ("loom.etl.schema", "ColumnSchema"),
        ("loom.etl.sql", "StepSQL"),
        ("loom.etl.storage", "StorageConfig"),
        ("loom.etl.temp", "IntermediateStore"),
        ("loom.etl.testing", "PolarsStepRunner"),
        ("loom.etl.compiler.validators", "validate_plan_catalog"),
        ("loom.etl.observability", "StructlogRunObserver"),
        ("loom.etl.observability.stores", "TableExecutionRecordStore"),
    ],
)
def test_package_entrypoints_support_reload_contracts(
    module_name: str,
    exported_symbol: str,
) -> None:
    module = _reload_module(module_name)
    assert exported_symbol in module.__all__


@pytest.mark.parametrize(
    ("module_name", "attribute_name"),
    [
        ("loom.etl.observability.records", "RunContext"),
        ("loom.etl.observability.observers.protocol", "ETLRunObserver"),
        ("loom.etl.observability.stores.protocol", "ExecutionRecordStore"),
        ("loom.etl.pipeline._params", "ETLParams"),
        ("loom.etl.storage._io", "TableDiscovery"),
        ("loom.etl.observability.config", "ObservabilityConfig"),
    ],
)
def test_internal_protocol_and_event_modules_support_reload_contracts(
    module_name: str,
    attribute_name: str,
) -> None:
    module = _reload_module(module_name)
    assert getattr(module, attribute_name) is not None

    temp_scope = _reload_module("loom.etl.temp._scope")
    assert temp_scope.TempScope.RUN.value == "run"


def test_reload_sql_and_compiler_definition_modules_with_basic_usage() -> None:
    modules = _reload_modules(
        "loom.etl.compiler._errors",
        "loom.etl.io.target",
        "loom.etl.sql._predicate",
        "loom.etl.sql._predicate_dialect",
        "loom.etl.sql._predicate_sql",
    )

    compiler_errors = modules["loom.etl.compiler._errors"]
    target_variants = modules["loom.etl.io.target"]
    predicate_nodes = modules["loom.etl.sql._predicate"]
    predicate_dialect = modules["loom.etl.sql._predicate_dialect"]
    predicate_sql = modules["loom.etl.sql._predicate_sql"]

    class _Step:
        __qualname__ = "ReloadStep"

    err = compiler_errors.ETLCompilationError.missing_target(_Step)  # type: ignore[arg-type]
    assert err.code is compiler_errors.ETLErrorCode.MISSING_TARGET
    assert "ReloadStep" in str(err)
    assert "TargetSpec" in target_variants.__all__

    class _Dialect:
        def column(self, name: str) -> str:
            return f"col:{name}"

        def literal(self, value: object) -> str:
            return f"lit:{value!r}"

        def eq(self, left: str, right: str) -> str:
            return f"{left}={right}"

        def ne(self, left: str, right: str) -> str:
            return f"{left}!={right}"

        def gt(self, left: str, right: str) -> str:
            return f"{left}>{right}"

        def ge(self, left: str, right: str) -> str:
            return f"{left}>={right}"

        def lt(self, left: str, right: str) -> str:
            return f"{left}<{right}"

        def le(self, left: str, right: str) -> str:
            return f"{left}<={right}"

        def in_(self, ref: str, values: tuple[object, ...]) -> str:
            return f"{ref} in {values!r}"

        def and_(self, left: str, right: str) -> str:
            return f"({left} and {right})"

        def or_(self, left: str, right: str) -> str:
            return f"({left} or {right})"

        def not_(self, operand: str) -> str:
            return f"(not {operand})"

    node = predicate_nodes.EqPred(left=1, right=1)
    rendered = predicate_dialect.fold_predicate(node, object(), _Dialect())
    assert rendered == "lit:1=lit:1"
    assert callable(predicate_sql.predicate_to_sql)


def test_runner_error_message_includes_requested_include_set() -> None:
    from loom.etl.runner.errors import InvalidStageError

    err = InvalidStageError(frozenset({"MissingStep", "MissingProcess"}))
    text = str(err)
    assert "No steps or processes match include=" in text
    assert "MissingStep" in text
    assert "MissingProcess" in text


def test_reload_source_and_target_modules_with_real_builder_calls() -> None:
    modules = _reload_modules("loom.etl.io.source._from", "loom.etl.io.target._into")
    source_mod = modules["loom.etl.io.source._from"]
    target_mod = modules["loom.etl.io.target._into"]

    from loom.etl.pipeline._proxy import params
    from loom.etl.schema._table import col

    source_spec = (
        source_mod.FromTable("raw.orders")
        .where(col("year") == params.run_date.year)
        .columns("id", "amount")
        .parse_json("payload", list[str])
        ._to_spec("orders")
    )
    assert source_spec.alias == "orders"
    assert source_spec.columns == ("id", "amount")
    assert source_spec.json_columns[0].column == "payload"

    file_spec = (
        source_mod.FromFile("s3://bucket/events.json", format=source_mod.Format.JSON)
        .columns("id")
        .parse_json("body", list[str])
        ._to_spec("events")
    )
    assert file_spec.path == "s3://bucket/events.json"
    assert file_spec.columns == ("id",)
    assert file_spec.json_columns[0].column == "body"

    target = target_mod.IntoTable("staging.orders").replace_partitions(
        values={"year": params.run_date.year}
    )
    compiled_target = target._to_spec()
    assert compiled_target.table_ref.ref == "staging.orders"
    assert compiled_target.replace_predicate is not None

    file_target = target_mod.IntoFile(
        "/var/lib/loom/out.csv", format=target_mod.Format.CSV
    )._to_spec()
    assert file_target.path == "/var/lib/loom/out.csv"

    temp_target = target_mod.IntoTemp("parts", append=True)._to_spec()
    assert temp_target.temp_name == "parts"


def test_reload_temp_store_module_and_helpers() -> None:
    store_mod = _reload_module("loom.etl.temp._store")
    polars_temp_mod = _reload_module("loom.etl.backends.polars._temp")
    spark_temp_mod = _reload_module("loom.etl.backends.spark._temp")

    fake_polars = type("LazyFrame", (), {"__module__": "polars.lazyframe.frame"})()
    fake_spark = type("DataFrame", (), {"__module__": "pyspark.sql.dataframe"})()

    assert polars_temp_mod._is_polars_lazy_frame(fake_polars)
    assert spark_temp_mod._is_spark_dataframe(fake_spark)

    base = "/var/lib/loom/test"
    single = polars_temp_mod._arrow_path("orders", base)
    part = polars_temp_mod._arrow_part("orders", base)
    assert single.endswith("/orders.arrow")
    assert "/orders/" in part and part.endswith(".arrow")

    class _Reader:
        def parquet(self, _path: str) -> object:
            analysis_exc = type(
                "AnalysisException",
                (Exception,),
                {"__module__": "pyspark.sql.utils"},
            )
            raise analysis_exc("missing")

    class _Spark:
        read = _Reader()

    assert spark_temp_mod._probe_spark(_Spark(), "/var/lib/loom/missing") is None

    # Verify _join_path still lives in _store for shared path construction
    assert store_mod._join_path("/var/lib/loom", "runs", "abc") == "/var/lib/loom/runs/abc"

    class _ReaderUnexpected:
        def parquet(self, _path: str) -> object:
            raise RuntimeError("boom")

    class _SparkUnexpected:
        read = _ReaderUnexpected()

    with pytest.raises(RuntimeError, match="boom"):
        spark_temp_mod._probe_spark(_SparkUnexpected(), "/var/lib/loom/missing")


def test_lazy_pipeline_exports_resolve() -> None:
    from loom.etl.pipeline import ETLParams, ETLProcess, ETLStep

    assert ETLParams is not None
    assert ETLStep is not None
    assert ETLProcess is not None


def test_lazy_sql_exports_resolve() -> None:
    from loom.etl.sql import StepSQL, resolve_sql, sql_literal

    assert StepSQL is not None
    assert callable(resolve_sql)
    assert callable(sql_literal)


def test_format_enum_values_are_stable() -> None:
    from loom.etl.io._format import Format

    assert Format.CSV.value == "csv"
    assert Format.DELTA.value == "delta"


def test_read_options_defaults_and_overrides() -> None:
    from loom.etl.io._read_options import CsvReadOptions, ExcelReadOptions, JsonReadOptions

    csv = CsvReadOptions()
    js = JsonReadOptions(infer_schema_length=None)
    xlsx = ExcelReadOptions(sheet_name="Data")
    assert csv.separator == ","
    assert csv.has_header is True
    assert js.infer_schema_length is None
    assert xlsx.sheet_name == "Data"


def test_write_options_defaults_and_overrides() -> None:
    from loom.etl.io._write_options import CsvWriteOptions, ParquetWriteOptions

    csv = CsvWriteOptions(separator=";")
    pq = ParquetWriteOptions(compression="zstd")
    assert csv.separator == ";"
    assert pq.compression == "zstd"


def test_target_variant_specs_store_expected_fields() -> None:
    from loom.etl.io._format import Format
    from loom.etl.io.target._file import FileSpec
    from loom.etl.io.target._table import AppendSpec, ReplaceSpec
    from loom.etl.io.target._temp import TempFanInSpec, TempSpec
    from loom.etl.schema._table import TableRef
    from loom.etl.temp._scope import TempScope

    table = TableRef("staging.orders")
    file_spec = FileSpec(path="/var/lib/loom/out.csv", format=Format.CSV)
    append = AppendSpec(table_ref=table)
    replace = ReplaceSpec(table_ref=table)
    temp = TempSpec(temp_name="tmp_orders", temp_scope=TempScope.RUN)
    fan_in = TempFanInSpec(temp_name="tmp_parts", temp_scope=TempScope.CORRELATION)

    assert file_spec.path == "/var/lib/loom/out.csv"
    assert append.table_ref == table
    assert replace.table_ref == table
    assert temp.temp_name == "tmp_orders"
    assert fan_in.temp_scope is TempScope.CORRELATION


def test_schema_structural_types_compose_normally() -> None:
    from loom.etl.schema._schema import (
        ColumnSchema,
        DatetimeType,
        ListType,
        LoomDtype,
        StructField,
        StructType,
    )

    payload = StructType(
        fields=(
            StructField("ts", DatetimeType("us", "UTC")),
            StructField("tags", ListType(inner=LoomDtype.UTF8)),
        )
    )
    schema = (
        ColumnSchema("id", LoomDtype.INT64, nullable=False),
        ColumnSchema("payload", payload),
    )
    assert schema[0].nullable is False
    assert isinstance(schema[1].dtype, StructType)


def test_observer_event_records_hold_runtime_data() -> None:
    from loom.etl.observability.records import (
        EventName,
        PipelineRunRecord,
        ProcessRunRecord,
        RunContext,
        RunStatus,
        StepRunRecord,
    )

    now = datetime.now(UTC)
    ctx = RunContext(run_id="run-1", correlation_id="corr-1", attempt=2, last_attempt=False)
    pipeline = PipelineRunRecord(
        event=EventName.PIPELINE_END,
        run_id=ctx.run_id,
        correlation_id=ctx.correlation_id,
        attempt=ctx.attempt,
        pipeline="MyPipeline",
        started_at=now,
        status=RunStatus.SUCCESS,
        duration_ms=100,
        error=None,
    )
    process = ProcessRunRecord(
        event=EventName.PROCESS_END,
        run_id=ctx.run_id,
        correlation_id=ctx.correlation_id,
        attempt=ctx.attempt,
        process_run_id="proc-1",
        process="MyProcess",
        started_at=now,
        status=RunStatus.SUCCESS,
        duration_ms=50,
        error=None,
    )
    step = StepRunRecord(
        event=EventName.STEP_END,
        run_id=ctx.run_id,
        correlation_id=ctx.correlation_id,
        attempt=ctx.attempt,
        step_run_id="step-1",
        step="MyStep",
        started_at=now,
        status=RunStatus.SUCCESS,
        duration_ms=10,
        error=None,
    )
    assert pipeline.pipeline == "MyPipeline"
    assert process.process_run_id == "proc-1"
    assert step.step_run_id == "step-1"


def test_storage_protocols_runtime_checkable_contracts() -> None:
    from loom.etl.schema._schema import ColumnSchema, LoomDtype
    from loom.etl.schema._table import TableRef
    from loom.etl.storage._io import SourceReader, TableDiscovery, TargetWriter

    class _Catalog:
        def exists(self, ref: TableRef) -> bool:
            return ref.ref == "raw.orders"

        def columns(self, ref: TableRef) -> tuple[str, ...]:
            return ("id",)

        def schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None:
            return (ColumnSchema("id", LoomDtype.INT64),)

        def update_schema(self, ref: TableRef, schema: tuple[ColumnSchema, ...]) -> None:
            return None

    class _Reader:
        def read(self, spec: object, params_instance: object) -> object:
            return object()

    class _Writer:
        def write(self, frame: object, spec: object, params_instance: object) -> None:
            return None

    assert isinstance(_Catalog(), TableDiscovery)
    assert isinstance(_Reader(), SourceReader)
    assert isinstance(_Writer(), TargetWriter)


def test_temp_scope_enum_values_are_stable() -> None:
    from loom.etl.temp._scope import TempScope

    assert TempScope.RUN.value == "run"
    assert TempScope.CORRELATION.value == "correlation"


def test_public_root_exports_are_importable() -> None:
    import loom.etl as etl

    for name in ("FromTable", "IntoTable", "ETLRunner", "TableRef", "StepSQL"):
        assert hasattr(etl, name)


def test_sql_module_rejects_unknown_lazy_symbol() -> None:
    import loom.etl.sql as sql

    with pytest.raises(AttributeError, match="has no attribute"):
        _ = sql.DOES_NOT_EXIST
