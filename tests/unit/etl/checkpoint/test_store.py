"""Tests for IntoTemp, FromTemp, CheckpointScope and CheckpointStore."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from loom.etl import (
    CheckpointScope,
    ETLParams,
    ETLPipeline,
    ETLProcess,
    ETLStep,
    FromTable,
    FromTemp,
    IntoTable,
    IntoTemp,
    Sources,
)
from loom.etl.checkpoint import CheckpointStore
from loom.etl.compiler import ETLCompilationError, ETLCompiler
from loom.etl.declarative.source import SourceKind
from loom.etl.testing import StubCatalog, StubSourceReader, StubTargetWriter


class P(ETLParams):  # type: ignore[misc]
    run_date: date


class ProduceStep(ETLStep[P]):
    sources = Sources(raw=FromTable("raw.orders"))
    target = IntoTemp("normalized")

    def execute(self, params: P, *, raw: Any) -> Any:  # type: ignore[override]
        return raw


class ConsumeStep(ETLStep[P]):
    normalized = FromTemp("normalized")
    target = IntoTable("mart.summary").replace()

    def execute(self, params: P, *, normalized: Any) -> Any:  # type: ignore[override]
        return normalized


class TempProcess(ETLProcess[P]):
    steps = [ProduceStep, ConsumeStep]


class TempPipeline(ETLPipeline[P]):
    processes = [TempProcess]


@pytest.fixture
def catalog() -> StubCatalog:
    return StubCatalog(tables={"raw.orders": (), "mart.summary": ()})


class TestTempSpecConstruction:
    @pytest.mark.parametrize(
        "scope",
        [CheckpointScope.RUN, CheckpointScope.CORRELATION],
    )
    def test_into_temp_scope_contract(self, scope: CheckpointScope) -> None:
        target = IntoTemp("orders", scope=scope)
        assert target.temp_name == "orders"
        assert target.scope is scope
        spec = target._to_spec()
        assert spec.temp_name == "orders"
        assert spec.temp_scope is scope

    def test_from_temp_to_spec_kind_is_temp(self) -> None:
        spec = FromTemp("orders")._to_spec("data")
        assert spec.kind is SourceKind.TEMP
        assert spec.temp_name == "orders"
        assert spec.alias == "data"


class TestCompilerTempValidation:
    @pytest.mark.parametrize(
        "step_type,check",
        [
            (
                ProduceStep,
                lambda plan: plan.target_binding.spec.temp_name == "normalized",
            ),
            (
                ConsumeStep,
                lambda plan: (
                    len(plan.source_bindings) == 1
                    and plan.source_bindings[0].alias == "normalized"
                    and plan.source_bindings[0].spec.kind is SourceKind.TEMP
                    and plan.source_bindings[0].spec.temp_name == "normalized"
                ),
            ),
        ],
    )
    def test_compile_step_temp_bindings(
        self,
        step_type: type[ETLStep[P]],
        check: Callable[[Any], bool],
    ) -> None:
        assert check(ETLCompiler().compile_step(step_type))

    def test_compile_pipeline_with_temp_passes(self, catalog: StubCatalog) -> None:
        assert ETLCompiler(catalog).compile(TempPipeline) is not None

    @pytest.mark.parametrize(
        "pipeline_builder,error",
        [
            (
                lambda: _build_orphan_pipeline(),
                "normalized",
            ),
            (
                lambda: _build_parallel_same_group_pipeline(),
                "normalized",
            ),
        ],
    )
    def test_compile_pipeline_temp_errors(
        self,
        catalog: StubCatalog,
        pipeline_builder: Callable[[], type[ETLPipeline[P]]],
        error: str,
    ) -> None:
        with pytest.raises(ETLCompilationError, match=error):
            ETLCompiler(catalog).compile(pipeline_builder())

    def test_compile_pipeline_sequential_step_sees_prior_parallel_temp(
        self,
        catalog: StubCatalog,
    ) -> None:
        ETLCompiler(catalog).compile(_build_parallel_then_consume_pipeline())


class TestCheckpointStore:
    @pytest.mark.parametrize(
        "cleanup_fn,subdir,key",
        [
            (lambda store, value: store.cleanup_run(value), "runs", "my-run"),
            (
                lambda store, value: store.cleanup_correlation(value),
                "correlations",
                "job-123",
            ),
        ],
    )
    def test_cleanup_removes_directory(
        self,
        tmp_path: Path,
        cleanup_fn: Callable[[CheckpointStore, str], None],
        subdir: str,
        key: str,
    ) -> None:
        store = CheckpointStore(root=str(tmp_path))
        directory = tmp_path / subdir / key
        directory.mkdir(parents=True)
        cleanup_fn(store, key)
        assert not directory.exists()

    def test_cleanup_run_is_noop_when_missing(self, tmp_path: Path) -> None:
        CheckpointStore(root=str(tmp_path)).cleanup_run("does-not-exist")

    @pytest.mark.parametrize(
        "scope,data,error,match",
        [
            (
                CheckpointScope.RUN,
                {"not": "a frame"},
                TypeError,
                "Polars backend expects polars.LazyFrame",
            ),
            (CheckpointScope.CORRELATION, object(), ValueError, "correlation_id"),
        ],
    )
    def test_store_put_validation(
        self,
        tmp_path: Path,
        scope: CheckpointScope,
        data: object,
        error: type[Exception],
        match: str,
    ) -> None:
        store = CheckpointStore(root=str(tmp_path))
        with pytest.raises(error, match=match):
            store.put("x", run_id="r", correlation_id=None, scope=scope, data=data)

    def test_store_get_raises_when_missing(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            CheckpointStore(root=str(tmp_path)).get("missing", run_id="r", correlation_id=None)


class TestRunnerTempCleanup:
    def test_runner_cleanup_correlation_without_temp_store_raises(self) -> None:
        from loom.etl.runner import ETLRunner

        runner = ETLRunner(StubSourceReader({}), StubTargetWriter())
        with pytest.raises(RuntimeError, match="tmp_root"):
            runner.cleanup_correlation("job-123")


def _build_orphan_pipeline() -> type[ETLPipeline[P]]:
    class OrphanProcess(ETLProcess[P]):
        steps = [ConsumeStep]

    class OrphanPipeline(ETLPipeline[P]):
        processes = [OrphanProcess]

    return OrphanPipeline


def _build_parallel_same_group_pipeline() -> type[ETLPipeline[P]]:
    class ParallelProcess(ETLProcess[P]):
        steps = [[ProduceStep, ConsumeStep]]

    class ParallelPipeline(ETLPipeline[P]):
        processes = [ParallelProcess]

    return ParallelPipeline


def _build_parallel_then_consume_pipeline() -> type[ETLPipeline[P]]:
    class ProduceA(ETLStep[P]):
        sources = Sources(raw=FromTable("raw.orders"))
        target = IntoTemp("norm_a")

        def execute(self, params: P, *, raw: Any) -> Any:  # type: ignore[override]
            return raw

    class ConsumeA(ETLStep[P]):
        norm_a = FromTemp("norm_a")
        target = IntoTable("mart.summary").replace()

        def execute(self, params: P, *, norm_a: Any) -> Any:  # type: ignore[override]
            return norm_a

    class Proc(ETLProcess[P]):
        steps = [[ProduceA], ConsumeA]

    class Pipeline(ETLPipeline[P]):
        processes = [Proc]

    return Pipeline
