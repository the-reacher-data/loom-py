"""Tests for ETLRunner plan filtering and constructors."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from loom.etl import ETLParams, ETLPipeline, ETLProcess, ETLStep, FromTable, IntoTable
from loom.etl._runner import ETLRunner, InvalidStageError, _filter_plan
from loom.etl.compiler import ETLCompiler
from loom.etl.compiler._plan import (
    ParallelProcessGroup,
    ParallelStepGroup,
    PipelinePlan,
    ProcessPlan,
    StepPlan,
)
from loom.etl.testing import StubCatalog, StubSourceReader, StubTargetWriter

RunnerFactory = Callable[..., ETLRunner]


class P(ETLParams):  # type: ignore[misc]
    run_date: date


class StepA(ETLStep[P]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.a").replace()

    def execute(self, params: P, *, orders: Any) -> Any:  # type: ignore[override]
        return orders


class StepB(ETLStep[P]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.b").replace()

    def execute(self, params: P, *, orders: Any) -> Any:  # type: ignore[override]
        return orders


class StepC(ETLStep[P]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.c").replace()

    def execute(self, params: P, *, orders: Any) -> Any:  # type: ignore[override]
        return orders


class ProcAB(ETLProcess[P]):
    steps = [StepA, StepB]


class ProcC(ETLProcess[P]):
    steps = [StepC]


class ProcParallel(ETLProcess[P]):
    steps = [[StepA, StepB]]


class PipelineAll(ETLPipeline[P]):
    processes = [ProcAB, ProcC]


class PipelineParallelProcs(ETLPipeline[P]):
    processes = [[ProcAB, ProcC]]


class PipelineParallelSteps(ETLPipeline[P]):
    processes = [ProcParallel]


@pytest.fixture
def params() -> P:
    return P(run_date=date(2024, 1, 1))


@pytest.fixture
def runner_factory() -> RunnerFactory:
    def _make(observers: Sequence[Any] | None = None) -> ETLRunner:
        catalog = StubCatalog(
            tables={
                "raw.orders": ("id",),
                "staging.a": ("id",),
                "staging.b": ("id",),
                "staging.c": ("id",),
            }
        )
        reader = StubSourceReader({"raw.orders": object()})
        return ETLRunner(
            reader,
            StubTargetWriter(),
            catalog,
            observers=list(observers or ()),
        )

    return _make


def _compile(pipeline_type: type[ETLPipeline[P]]) -> PipelinePlan:
    return ETLCompiler().compile(pipeline_type)


class TestFilterPlan:
    @pytest.mark.parametrize(
        "include,expected_process,expected_steps",
        [
            (frozenset({"StepA"}), ProcAB, ("StepA",)),
            (frozenset({"StepA", "StepB"}), ProcAB, ("StepA", "StepB")),
            (frozenset({"ProcAB"}), ProcAB, ("StepA", "StepB")),
            (frozenset({"ProcC"}), ProcC, ("StepC",)),
        ],
    )
    def test_filter_regular_pipeline(
        self,
        include: frozenset[str],
        expected_process: type[ETLProcess[P]],
        expected_steps: tuple[str, ...],
    ) -> None:
        filtered = _filter_plan(_compile(PipelineAll), include)
        assert len(filtered.nodes) == 1
        proc = filtered.nodes[0]
        assert isinstance(proc, ProcessPlan)
        assert proc.process_type is expected_process
        step_names = tuple(
            node.step_type.__name__ for node in proc.nodes if isinstance(node, StepPlan)
        )
        assert step_names == expected_steps

    @pytest.mark.parametrize(
        "pipeline_type,include,expect_parallel,expected_len,first_step",
        [
            (PipelineParallelSteps, frozenset({"StepA"}), False, 1, "StepA"),
            (PipelineParallelSteps, frozenset({"StepA", "StepB"}), True, 2, None),
            (PipelineParallelProcs, frozenset({"ProcC"}), False, 1, None),
            (PipelineParallelProcs, frozenset({"ProcAB", "ProcC"}), True, 2, None),
        ],
    )
    def test_filter_parallel_shapes(
        self,
        pipeline_type: type[ETLPipeline[P]],
        include: frozenset[str],
        expect_parallel: bool,
        expected_len: int,
        first_step: str | None,
    ) -> None:
        filtered = _filter_plan(_compile(pipeline_type), include)
        top_node = filtered.nodes[0]

        if pipeline_type is PipelineParallelSteps:
            assert isinstance(top_node, ProcessPlan)
            node = top_node.nodes[0]
            if expect_parallel:
                assert isinstance(node, ParallelStepGroup)
                assert len(node.plans) == expected_len
            else:
                assert isinstance(node, StepPlan)
                assert node.step_type.__name__ == first_step
            return

        if expect_parallel:
            assert isinstance(top_node, ParallelProcessGroup)
            assert len(top_node.plans) == expected_len
            return

        assert isinstance(top_node, ProcessPlan)
        assert top_node.process_type is ProcC

    def test_filter_no_match_raises(self) -> None:
        with pytest.raises(InvalidStageError):
            _filter_plan(_compile(PipelineAll), frozenset({"NonExistent"}))


class TestRunnerRun:
    @pytest.mark.parametrize(
        "include,expected_steps",
        [
            (None, 3),
            (["StepA"], 1),
        ],
    )
    def test_runner_run_counts_steps(
        self,
        params: P,
        runner_factory: RunnerFactory,
        include: list[str] | None,
        expected_steps: int,
    ) -> None:
        from loom.etl.testing import StubRunObserver

        observer = StubRunObserver()
        runner_factory([observer]).run(PipelineAll, params, include=include)
        assert len(observer.step_statuses) == expected_steps

    def test_runner_run_invalid_include_raises(
        self,
        params: P,
        runner_factory: RunnerFactory,
    ) -> None:
        with pytest.raises(InvalidStageError):
            runner_factory().run(PipelineAll, params, include=["DoesNotExist"])


class TestRunnerFromConfig:
    def test_from_config_unity_catalog_without_spark_raises(self) -> None:
        from loom.etl._storage_config import UnityCatalogConfig

        with pytest.raises(ValueError, match="SparkSession"):
            ETLRunner.from_config(UnityCatalogConfig(type="unity_catalog"))

    def test_from_config_unity_catalog_with_spark_builds_spark_backends(self) -> None:
        pytest.importorskip("pyspark")

        from unittest.mock import MagicMock

        from loom.etl._storage_config import UnityCatalogConfig
        from loom.etl.backends.spark._catalog import SparkCatalog
        from loom.etl.backends.spark._reader import SparkDeltaReader

        spark = MagicMock()
        runner = ETLRunner.from_config(UnityCatalogConfig(type="unity_catalog"), spark=spark)

        assert isinstance(runner._executor._reader, SparkDeltaReader)
        assert isinstance(runner._compiler._catalog, SparkCatalog)

    @pytest.mark.parametrize(
        "tmp_suffix,has_temp_store",
        [
            (None, False),
            ("tmp", True),
        ],
    )
    def test_from_config_delta_builds_polars_backends(
        self,
        tmp_path: Path,
        tmp_suffix: str | None,
        has_temp_store: bool,
    ) -> None:
        from loom.etl._storage_config import DeltaConfig
        from loom.etl.backends.polars._reader import PolarsDeltaReader
        from loom.etl.backends.polars._writer import PolarsDeltaWriter

        config = DeltaConfig(
            root=str(tmp_path),
            tmp_root=str(tmp_path / tmp_suffix) if tmp_suffix else "",
        )
        runner = ETLRunner.from_config(config)

        assert isinstance(runner._executor._reader, PolarsDeltaReader)
        assert isinstance(runner._executor._writer, PolarsDeltaWriter)
        assert (runner._temp_store is not None) is has_temp_store


class TestRunnerFromDict:
    def test_from_dict_delta_builds_polars_backends(self, tmp_path: Path) -> None:
        from loom.etl.backends.polars._reader import PolarsDeltaReader

        runner = ETLRunner.from_dict({"root": str(tmp_path)})
        assert isinstance(runner._executor._reader, PolarsDeltaReader)

    @pytest.mark.parametrize(
        "storage,error,match",
        [
            ({"type": "unity_catalog"}, ValueError, "SparkSession"),
            ({"type": "unknown_backend"}, ValueError, "Unknown storage backend"),
        ],
    )
    def test_from_dict_errors(
        self, storage: dict[str, Any], error: type[Exception], match: str
    ) -> None:
        with pytest.raises(error, match=match):
            ETLRunner.from_dict(storage)

    def test_from_dict_invalid_storage_shape_raises(self) -> None:
        import msgspec

        with pytest.raises(msgspec.ValidationError):
            ETLRunner.from_dict({"root": 123})

    def test_from_dict_with_observability_dict(self, tmp_path: Path) -> None:
        runner = ETLRunner.from_dict(
            {"root": str(tmp_path)},
            observability={"log": False},
        )
        assert runner is not None


class TestRunnerCleaner:
    def test_from_config_with_injected_cleaner(self, tmp_path: Path) -> None:
        from loom.etl._storage_config import DeltaConfig

        deleted: list[str] = []

        class SpyCleaner:
            def delete_tree(self, path: str) -> None:
                deleted.append(path)

        config = DeltaConfig(root=str(tmp_path), tmp_root=str(tmp_path / "tmp"))
        runner = ETLRunner.from_config(config, cleaner=SpyCleaner())

        assert runner._temp_store is not None
        runner._temp_store.cleanup_run("run-xyz")
        assert any("run-xyz" in path for path in deleted)
