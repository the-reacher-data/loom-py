from __future__ import annotations

import importlib
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import msgspec
import pytest

from loom.etl.schema._table import TableRef


def _reload_modules(*names: str) -> dict[str, ModuleType]:
    return {name: importlib.reload(importlib.import_module(name)) for name in names}


def _build_step_plan(plan_mod: ModuleType, source_binding: Any, target_spec: Any, name: str) -> Any:
    return plan_mod.StepPlan(
        step_type=type(name, (), {}),
        params_type=object,
        source_bindings=(source_binding,),
        target_binding=plan_mod.TargetBinding(spec=target_spec),
    )


class _CaptureObserver:
    def __init__(self) -> None:
        self.called = 0

    def on_step_end(self, _step_run_id: str, _status: Any, _duration_ms: int) -> None:
        self.called += 1

    def __getattr__(self, _name: str) -> Any:
        return lambda *args, **kwargs: None


class _FailingObserver(_CaptureObserver):
    def on_step_end(self, _step_run_id: str, _status: Any, _duration_ms: int) -> None:
        raise RuntimeError("boom")


class _StructlogCapture:
    def __init__(self) -> None:
        self.logged: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def __getattr__(self, _name: str) -> Any:
        def _capture(*args: Any, **kwargs: Any) -> None:
            self.logged.append((args, kwargs))

        return _capture


def _spy_cleaner() -> Any:
    paths: list[str] = []
    return SimpleNamespace(paths=paths, delete_tree=paths.append)


class _Sink:
    def __init__(self) -> None:
        self.records: list[Any] = []

    def write_record(self, record: Any) -> None:
        self.records.append(record)


def test_binding_and_pipeline_runtime_contracts(monkeypatch: pytest.MonkeyPatch) -> None:
    mods = _reload_modules(
        "loom.etl.io.source._from",
        "loom.etl.io.source",
        "loom.etl.io.target._into",
        "loom.etl.io.target",
        "loom.etl.pipeline._params",
        "loom.etl.pipeline._pipeline",
        "loom.etl.pipeline._process",
        "loom.etl.pipeline._step",
        "loom.etl.compiler._binding",
    )
    binding_mod = mods["loom.etl.compiler._binding"]
    source_mod = mods["loom.etl.io.source._from"]
    target_mod = mods["loom.etl.io.target._into"]
    params_mod = mods["loom.etl.pipeline._params"]
    pipeline_mod = mods["loom.etl.pipeline._pipeline"]
    process_mod = mods["loom.etl.pipeline._process"]
    step_mod = mods["loom.etl.pipeline._step"]

    class P(params_mod.ETLParams):
        run_date: date

    class InlineStep(step_mod.ETLStep[P]):
        orders = source_mod.FromTable("raw.orders")
        target = target_mod.IntoTable("staging.orders").replace()

        def execute(self, params: P, *, orders: Any) -> Any:
            return orders

    class GroupedStep(step_mod.ETLStep[P]):
        sources = source_mod.Sources(orders=source_mod.FromTable("raw.orders"))
        target = target_mod.IntoTable("staging.grouped").replace()

        def execute(self, params: P, *, orders: Any) -> Any:
            return orders

    class NoSourceStep(step_mod.ETLStep[P]):
        target = target_mod.IntoTable("staging.none").replace()

        def execute(self, params: P) -> Any:
            return None

    inline = binding_mod.resolve_source_bindings(InlineStep)
    grouped = binding_mod.resolve_source_bindings(GroupedStep)
    none = binding_mod.resolve_source_bindings(NoSourceStep)

    assert len(inline) == 1 and inline[0].alias == "orders"
    assert len(grouped) == 1 and grouped[0].alias == "orders"
    assert none == ()
    assert binding_mod.resolve_target_binding(InlineStep).spec.table_ref.ref == "staging.orders"

    class BadTargetStep(step_mod.ETLStep[P]):
        target = object()

        def execute(self, params: P) -> Any:
            return None

    with pytest.raises(Exception, match="'target' must be"):
        binding_mod.resolve_target_binding(BadTargetStep)

    class BadGroupedStep(step_mod.ETLStep[P]):
        target = target_mod.IntoTable("staging.bad").replace()

        def execute(self, params: P) -> Any:
            return None

    monkeypatch.setattr(BadGroupedStep, "_source_form", step_mod._SourceForm.GROUPED)
    monkeypatch.setattr(BadGroupedStep, "sources", 42)
    with pytest.raises(Exception, match="sources"):
        binding_mod.resolve_source_bindings(BadGroupedStep)

    class Proc(process_mod.ETLProcess[P]):
        steps = [InlineStep]

    class Pipe(pipeline_mod.ETLPipeline[P]):
        processes = [Proc]

    assert Proc._params_type is P
    assert Pipe._params_type is P


def test_plan_and_schema_runtime_contracts() -> None:
    mods = _reload_modules(
        "loom.etl.compiler._plan",
        "loom.etl.io.source._specs",
        "loom.etl.io.target._table",
        "loom.etl.schema._table",
    )
    plan_mod = mods["loom.etl.compiler._plan"]
    source_mod = mods["loom.etl.io.source._specs"]
    target_table_mod = mods["loom.etl.io.target._table"]
    table_mod = mods["loom.etl.schema._table"]

    source_spec = source_mod.TableSourceSpec(
        alias="orders",
        table_ref=table_mod.TableRef("raw.orders"),
    )
    source_binding = plan_mod.SourceBinding(alias="orders", spec=source_spec)
    step_a = _build_step_plan(
        plan_mod,
        source_binding,
        target_table_mod.ReplaceSpec(table_ref=table_mod.TableRef("staging.a")),
        "StepA",
    )
    step_b = _build_step_plan(
        plan_mod,
        source_binding,
        target_table_mod.ReplaceSpec(table_ref=table_mod.TableRef("staging.b")),
        "StepB",
    )
    proc = plan_mod.ProcessPlan(
        process_type=type("Proc", (), {}),
        params_type=object,
        nodes=(step_a, plan_mod.ParallelStepGroup(plans=(step_b,))),
    )
    pipeline = plan_mod.PipelinePlan(
        pipeline_type=type("Pipe", (), {}),
        params_type=object,
        nodes=(proc, plan_mod.ParallelProcessGroup(plans=(proc,))),
    )

    assert [p.process_type.__name__ for p in plan_mod.iter_processes(pipeline)] == ["Proc", "Proc"]
    assert [s.step_type.__name__ for s in plan_mod.iter_steps_in_process(proc)] == [
        "StepA",
        "StepB",
    ]
    assert [s.step_type.__name__ for s in plan_mod.iter_all_steps(pipeline)] == [
        "StepA",
        "StepB",
        "StepA",
        "StepB",
    ]

    mapped_steps = plan_mod._map_process_nodes(
        proc.nodes,
        lambda step: step if step.step_type.__name__ == "StepB" else None,
    )
    assert len(mapped_steps) == 1

    mapped_processes = plan_mod._map_pipeline_nodes(
        pipeline.nodes,
        lambda p: p if p.process_type.__name__ == "Proc" else None,
    )
    assert len(mapped_processes) == 2

    visited_steps: list[str] = []
    plan_mod.visit_process_nodes(proc.nodes, lambda s: visited_steps.append(s.step_type.__name__))
    assert visited_steps == ["StepA", "StepB"]

    visited_processes: list[str] = []
    plan_mod.visit_pipeline_nodes(
        pipeline.nodes, lambda p: visited_processes.append(p.process_type.__name__)
    )
    assert visited_processes == ["Proc", "Proc"]

    orders = table_mod.TableRef("raw.orders")
    assert orders.c.year.table == orders
    assert table_mod.col("year").name == "year"
    with pytest.raises(AttributeError):
        _ = orders.c._private


def test_storage_config_and_temp_cleaners_runtime_contracts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mods = _reload_modules("loom.etl.storage._config", "loom.etl.temp._cleaners")
    config_mod = mods["loom.etl.storage._config"]
    cleaners_mod = mods["loom.etl.temp._cleaners"]

    storage = config_mod.convert_storage_config(
        {"defaults": {"table_path": {"uri": str(tmp_path / "lake")}}}
    )
    assert isinstance(storage, config_mod.StorageConfig)
    assert storage.to_path_locator().locate(TableRef("raw.orders")).uri.endswith("raw/orders")

    spark_storage = config_mod.convert_storage_config({"engine": "spark"})
    assert isinstance(spark_storage, config_mod.StorageConfig)
    assert spark_storage.engine == "spark"

    with pytest.raises(msgspec.ValidationError):
        config_mod.convert_storage_config({"engine": "unknown"})

    root = tmp_path / "cleanup-local"
    root.mkdir()
    (root / "a.txt").write_text("x", encoding="utf-8")
    cleaners_mod.LocalTempCleaner().delete_tree(str(root))
    assert not root.exists()

    auto = cleaners_mod.AutoTempCleaner()
    local_spy = _spy_cleaner()
    cloud_spy = _spy_cleaner()
    monkeypatch.setattr(auto, "_local", local_spy)
    monkeypatch.setattr(auto, "_cloud", cloud_spy)
    local_path = str(tmp_path / "cleanup-auto-local")
    auto.delete_tree(local_path)
    auto.delete_tree("s3://bucket/tmp")
    assert local_spy.paths == [local_path]
    assert cloud_spy.paths == ["s3://bucket/tmp"]

    removed: list[tuple[str, bool]] = []

    class _Fs:
        def exists(self, path: str) -> bool:
            return path == "bucket/path"

        def rm(self, path: str, recursive: bool) -> None:
            removed.append((path, recursive))

    fake_fsspec = SimpleNamespace(core=SimpleNamespace(url_to_fs=lambda _: (_Fs(), "bucket/path")))
    monkeypatch.setitem(sys.modules, "fsspec", fake_fsspec)
    cleaners_mod.FsspecTempCleaner().delete_tree("s3://bucket/path")
    assert removed == [("bucket/path", True)]

    assert cleaners_mod._is_cloud_path("abfss://container/path")
    assert not cleaners_mod._is_cloud_path("/var/lib/loom/path")


def test_proxy_and_locator_runtime_contracts() -> None:
    mods = _reload_modules("loom.etl.pipeline._proxy", "loom.etl.storage._locator")
    proxy_mod = mods["loom.etl.pipeline._proxy"]
    locator_mod = mods["loom.etl.storage._locator"]

    expr = proxy_mod.params.run_date.year
    assert repr(proxy_mod.params) == "params"
    assert repr(expr) == "params.run_date.year"
    assert (
        proxy_mod.resolve_param_expr(expr, SimpleNamespace(run_date=SimpleNamespace(year=2026)))
        == 2026
    )
    with pytest.raises(AttributeError):
        _ = proxy_mod.params._private

    prefix = locator_mod.PrefixLocator("/var/lib/loom/lake")
    assert prefix.locate(TableRef("raw.orders")).uri == "/var/lib/loom/lake/raw/orders"

    mapped = locator_mod.MappingLocator(
        mapping={"raw.orders": locator_mod.TableLocation(uri="s3://raw/orders")},
        default=locator_mod.TableLocation(uri="s3://default"),
    )
    assert mapped.locate(TableRef("raw.orders")).uri == "s3://raw/orders"
    assert mapped.locate(TableRef("staging.daily")).uri == "s3://default/staging/daily"
    with pytest.raises(KeyError):
        locator_mod.MappingLocator(mapping={}).locate(TableRef("missing.table"))

    assert isinstance(locator_mod._as_locator("/var/lib/loom/lake"), locator_mod.PrefixLocator)
    assert locator_mod._as_location("/var/lib/loom/runs").uri == "/var/lib/loom/runs"


def test_observer_runtime_contracts(monkeypatch: pytest.MonkeyPatch) -> None:
    mods = _reload_modules(
        "loom.etl.observability.observers.composite",
        "loom.etl.observability.records",
        "loom.etl.observability.observers.execution_records",
        "loom.etl.observability.observers.structlog",
        "loom.etl.observability.stores.table",
    )
    composite_mod = mods["loom.etl.observability.observers.composite"]
    events_mod = mods["loom.etl.observability.records"]
    sink_observer_mod = mods["loom.etl.observability.observers.execution_records"]
    structlog_mod = mods["loom.etl.observability.observers.structlog"]
    delta_sink_mod = mods["loom.etl.observability.stores.table"]

    capture = _CaptureObserver()
    composite = composite_mod.CompositeObserver([_FailingObserver(), capture])
    with pytest.MonkeyPatch.context() as mp:
        log = SimpleNamespace(error=lambda *args, **kwargs: None)
        mp.setattr(composite_mod, "_log", log)
        composite.on_step_end("s1", events_mod.RunStatus.SUCCESS, 1)
    assert capture.called == 1

    with pytest.MonkeyPatch.context() as mp:
        logger = _StructlogCapture()
        mp.setattr(structlog_mod, "_log", logger)
        observer = structlog_mod.StructlogRunObserver(slow_step_threshold_ms=10)
        spec = SimpleNamespace(
            table_ref=SimpleNamespace(ref="raw.orders"), temp_name=None, path=None, mode="replace"
        )
        plan = SimpleNamespace(
            pipeline_type=SimpleNamespace(__name__="Pipe"),
            process_type=SimpleNamespace(__name__="Proc"),
            step_type=SimpleNamespace(__name__="Step"),
            source_bindings=(SimpleNamespace(spec=spec),),
            target_binding=SimpleNamespace(spec=spec),
            nodes=(object(),),
        )
        ctx = events_mod.RunContext(run_id="r1", correlation_id="c1", attempt=2, last_attempt=False)
        observer.on_pipeline_start(plan, object(), ctx)
        observer.on_process_start(plan, ctx, "p1")
        observer.on_step_start(plan, ctx, "s1")
        observer.on_step_end("s1", events_mod.RunStatus.SUCCESS, 11)
        observer.on_step_error("s1", RuntimeError("x"))
        observer.on_process_end("p1", events_mod.RunStatus.SUCCESS, 2)
        observer.on_pipeline_end(ctx, events_mod.RunStatus.SUCCESS, 3)
    assert logger.logged

    sink = _Sink()
    sink_observer = sink_observer_mod.ExecutionRecordsObserver(sink)
    ctx = events_mod.RunContext(run_id="r2")
    pipeline_plan = SimpleNamespace(pipeline_type=SimpleNamespace(__name__="Pipe"))
    process_plan = SimpleNamespace(process_type=SimpleNamespace(__name__="Proc"))
    step_plan = SimpleNamespace(step_type=SimpleNamespace(__name__="Step"))
    sink_observer.on_pipeline_start(pipeline_plan, object(), ctx)
    sink_observer.on_process_start(process_plan, ctx, "proc-1")
    sink_observer.on_step_start(step_plan, ctx, "step-1")
    sink_observer.on_step_error("step-1", RuntimeError("boom"))
    sink_observer.on_step_end("step-1", events_mod.RunStatus.FAILED, 5)
    sink_observer.on_process_end("proc-1", events_mod.RunStatus.FAILED, 6)
    sink_observer.on_pipeline_end(ctx, events_mod.RunStatus.FAILED, 7)
    assert len(sink.records) == 3

    class _AppendWriter:
        def __init__(self) -> None:
            self.calls: list[tuple[Any, Any]] = []

        def write_record(self, record: Any, table_ref: Any, /) -> None:
            self.calls.append((record, table_ref))

    _ = monkeypatch
    append_writer = _AppendWriter()
    delta_sink = delta_sink_mod.TableExecutionRecordStore(append_writer)
    record = events_mod.StepRunRecord(
        event=events_mod.EventName.STEP_END,
        run_id="r3",
        correlation_id=None,
        attempt=1,
        step_run_id="s3",
        step="Step",
        started_at=datetime.now(tz=UTC),
        status=events_mod.RunStatus.SUCCESS,
        duration_ms=1,
        error=None,
    )
    delta_sink.write_record(record)
    assert append_writer.calls
    assert append_writer.calls[0][1].ref == "step_runs"
    with pytest.raises(TypeError):
        delta_sink.write_record(object())  # type: ignore[arg-type]


def test_testing_runtime_contracts(monkeypatch: pytest.MonkeyPatch) -> None:
    pl = pytest.importorskip("polars")

    mods = _reload_modules(
        "loom.etl.testing._result",
        "loom.etl.testing._runners",
        "loom.etl.testing._scenario",
        "loom.etl.testing._stubs",
    )
    result_mod = mods["loom.etl.testing._result"]
    runners_mod = mods["loom.etl.testing._runners"]
    scenario_mod = mods["loom.etl.testing._scenario"]
    stubs_mod = mods["loom.etl.testing._stubs"]

    result = result_mod.StepResult(pl.DataFrame({"id": [1], "v": [1.0]}))
    result.assert_count(1)
    result.assert_not_empty()
    assert result.to_polars().shape == (1, 2)

    class _Runner:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def seed(self, ref: str, data: list[tuple[Any, ...]], columns: list[str]) -> None:
            self.calls.append(ref)

    scenario = scenario_mod.ETLScenario().with_table("raw.orders", [(1,)], ["id"])
    runner = _Runner()
    scenario.apply(runner)
    assert runner.calls == ["raw.orders"]
    assert isinstance(runner, scenario_mod.StepRunnerProto)

    catalog = stubs_mod.StubCatalog(tables={"raw.orders": ("id",)})
    assert catalog.exists(TableRef("raw.orders"))
    assert catalog.columns(TableRef("raw.orders")) == ("id",)
    assert catalog.schema(TableRef("raw.orders")) is not None
    reader = stubs_mod.StubSourceReader({"orders": object()})
    writer = stubs_mod.StubTargetWriter()
    observer = stubs_mod.StubRunObserver()
    assert reader.read(SimpleNamespace(alias="orders"), None) is not None
    writer.write(object(), SimpleNamespace(mode="replace"), None)
    observer.on_pipeline_start(SimpleNamespace(), object(), SimpleNamespace(run_id="r1"))
    assert writer.written

    lazy = runners_mod._build_lazy_frame([(1, 2.0)], ["id", "v"]).collect()
    assert lazy.shape == (1, 2)
    capturing = runners_mod._PolarsCapturingWriter()
    capturing.write(pl.DataFrame({"id": [1]}).lazy(), SimpleNamespace(mode="replace"), None)
    assert capturing.spec is not None
    stub = runners_mod._PolarsStubReader({"raw.orders": pl.DataFrame({"id": [1]}).lazy()})
    assert (
        stub.read(SimpleNamespace(table_ref=TableRef("raw.orders"), alias="orders"), None)
        is not None
    )

    polars_runner = runners_mod.PolarsStepRunner().seed("raw.orders", [(1,)], ["id"])
    with pytest.raises(RuntimeError, match="No spec"):
        _ = polars_runner.target_spec

    monkeypatch.setattr(runners_mod.ETLCompiler, "compile_step", lambda _self, _cls: object())
    monkeypatch.setattr(runners_mod.ETLExecutor, "run_step", lambda _self, _plan, _params: None)
    with pytest.raises(RuntimeError, match="Step produced no output"):
        polars_runner.run(type("DummyStep", (), {}), object())
