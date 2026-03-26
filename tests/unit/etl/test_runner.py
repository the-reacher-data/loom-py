"""Tests for ETLRunner — plan filtering and from_config wiring."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from loom.etl import ETLParams, ETLPipeline, ETLProcess, ETLStep, FromTable, IntoTable
from loom.etl._runner import InvalidStageError, _filter_plan
from loom.etl.compiler import ETLCompiler
from loom.etl.compiler._plan import ParallelProcessGroup, ParallelStepGroup, ProcessPlan, StepPlan
from loom.etl.testing import StubCatalog, StubSourceReader, StubTargetWriter


class P(ETLParams):
    run_date: date


PARAMS = P(run_date=date(2024, 1, 1))


# ---------------------------------------------------------------------------
# Step / process / pipeline fixtures
# ---------------------------------------------------------------------------


class StepA(ETLStep[P]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.a").replace()

    def execute(self, params: P, *, orders: Any) -> Any:
        return orders


class StepB(ETLStep[P]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.b").replace()

    def execute(self, params: P, *, orders: Any) -> Any:
        return orders


class StepC(ETLStep[P]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.c").replace()

    def execute(self, params: P, *, orders: Any) -> Any:
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


_compiler = ETLCompiler()


# ---------------------------------------------------------------------------
# _filter_plan — step-level filtering
# ---------------------------------------------------------------------------


def test_filter_by_step_name_keeps_matching_step() -> None:
    plan = _compiler.compile(PipelineAll)
    filtered = _filter_plan(plan, frozenset({"StepA"}))
    proc = filtered.nodes[0]
    assert isinstance(proc, ProcessPlan)
    assert len(proc.nodes) == 1
    assert isinstance(proc.nodes[0], StepPlan)
    assert proc.nodes[0].step_type is StepA


def test_filter_by_step_name_drops_empty_process() -> None:
    plan = _compiler.compile(PipelineAll)
    filtered = _filter_plan(plan, frozenset({"StepA"}))
    # ProcC (only StepC) must be gone
    assert len(filtered.nodes) == 1


def test_filter_keeps_multiple_matching_steps() -> None:
    plan = _compiler.compile(PipelineAll)
    filtered = _filter_plan(plan, frozenset({"StepA", "StepB"}))
    proc = filtered.nodes[0]
    assert isinstance(proc, ProcessPlan)
    assert len(proc.nodes) == 2


# ---------------------------------------------------------------------------
# _filter_plan — process-level filtering
# ---------------------------------------------------------------------------


def test_filter_by_process_name_keeps_all_steps() -> None:
    plan = _compiler.compile(PipelineAll)
    filtered = _filter_plan(plan, frozenset({"ProcAB"}))
    assert len(filtered.nodes) == 1
    proc = filtered.nodes[0]
    assert isinstance(proc, ProcessPlan)
    assert proc.process_type is ProcAB
    assert len(proc.nodes) == 2  # both StepA and StepB


def test_filter_by_process_name_drops_other_processes() -> None:
    plan = _compiler.compile(PipelineAll)
    filtered = _filter_plan(plan, frozenset({"ProcC"}))
    assert len(filtered.nodes) == 1
    proc = filtered.nodes[0]
    assert isinstance(proc, ProcessPlan)
    assert proc.process_type is ProcC


# ---------------------------------------------------------------------------
# _filter_plan — parallel group collapsing
# ---------------------------------------------------------------------------


def test_filter_parallel_steps_single_match_flattens_to_step_plan() -> None:
    plan = _compiler.compile(PipelineParallelSteps)
    filtered = _filter_plan(plan, frozenset({"StepA"}))
    proc = filtered.nodes[0]
    assert isinstance(proc, ProcessPlan)
    node = proc.nodes[0]
    # Single survivor must be flattened out of the ParallelStepGroup
    assert isinstance(node, StepPlan)
    assert node.step_type is StepA


def test_filter_parallel_steps_multiple_matches_keeps_group() -> None:
    plan = _compiler.compile(PipelineParallelSteps)
    filtered = _filter_plan(plan, frozenset({"StepA", "StepB"}))
    proc = filtered.nodes[0]
    node = proc.nodes[0]
    assert isinstance(node, ParallelStepGroup)
    assert len(node.plans) == 2


def test_filter_parallel_procs_single_match_flattens_to_process_plan() -> None:
    plan = _compiler.compile(PipelineParallelProcs)
    filtered = _filter_plan(plan, frozenset({"ProcC"}))
    node = filtered.nodes[0]
    assert isinstance(node, ProcessPlan)
    assert node.process_type is ProcC


def test_filter_parallel_procs_multiple_matches_keeps_group() -> None:
    plan = _compiler.compile(PipelineParallelProcs)
    filtered = _filter_plan(plan, frozenset({"ProcAB", "ProcC"}))
    node = filtered.nodes[0]
    assert isinstance(node, ParallelProcessGroup)
    assert len(node.plans) == 2


# ---------------------------------------------------------------------------
# _filter_plan — no match raises
# ---------------------------------------------------------------------------


def test_filter_no_match_raises_invalid_stage_error() -> None:
    plan = _compiler.compile(PipelineAll)
    with pytest.raises(InvalidStageError):
        _filter_plan(plan, frozenset({"NonExistent"}))


# ---------------------------------------------------------------------------
# ETLRunner.run — end-to-end with stubs
# ---------------------------------------------------------------------------


_CATALOG = StubCatalog(
    tables={
        "raw.orders": ("id",),
        "staging.a": ("id",),
        "staging.b": ("id",),
        "staging.c": ("id",),
    }
)
_READER = StubSourceReader({"raw.orders": object()})


def _make_runner(observers: Any = ()) -> Any:
    from loom.etl._runner import ETLRunner

    return ETLRunner(_READER, StubTargetWriter(), _CATALOG, observers=observers)


def test_runner_run_all_stages() -> None:
    from loom.etl.testing import StubRunObserver

    obs = StubRunObserver()
    _make_runner(observers=[obs]).run(PipelineAll, PARAMS)
    assert len(obs.step_statuses) == 3  # StepA, StepB, StepC


def test_runner_run_with_include_filters_steps() -> None:
    from loom.etl.testing import StubRunObserver

    obs = StubRunObserver()
    _make_runner(observers=[obs]).run(PipelineAll, PARAMS, include=["StepA"])
    assert len(obs.step_statuses) == 1


def test_runner_run_with_include_invalid_raises() -> None:
    with pytest.raises(InvalidStageError):
        _make_runner().run(PipelineAll, PARAMS, include=["DoesNotExist"])


# ---------------------------------------------------------------------------
# ETLRunner.from_config — Unity Catalog wiring
# ---------------------------------------------------------------------------


def test_from_config_unity_catalog_without_spark_raises() -> None:
    from loom.etl._runner import ETLRunner
    from loom.etl._storage_config import UnityCatalogConfig

    config = UnityCatalogConfig(type="unity_catalog")
    with pytest.raises(ValueError, match="SparkSession"):
        ETLRunner.from_config(config)


def test_from_config_unity_catalog_with_spark_builds_spark_backends() -> None:
    pytest.importorskip("pyspark")

    from unittest.mock import MagicMock

    from loom.etl._runner import ETLRunner
    from loom.etl._storage_config import UnityCatalogConfig
    from loom.etl.backends.spark._catalog import SparkCatalog
    from loom.etl.backends.spark._reader import SparkDeltaReader

    spark = MagicMock()
    config = UnityCatalogConfig(type="unity_catalog")
    runner = ETLRunner.from_config(config, spark=spark)

    assert isinstance(runner._executor._reader, SparkDeltaReader)
    assert isinstance(runner._compiler._catalog, SparkCatalog)


# ---------------------------------------------------------------------------
# ETLRunner.from_config — DeltaConfig programmatic construction
# ---------------------------------------------------------------------------


def test_from_config_delta_builds_polars_backends(tmp_path: Any) -> None:
    from loom.etl._runner import ETLRunner
    from loom.etl._storage_config import DeltaConfig
    from loom.etl.backends.polars._reader import PolarsDeltaReader
    from loom.etl.backends.polars._writer import PolarsDeltaWriter

    config = DeltaConfig(root=str(tmp_path))
    runner = ETLRunner.from_config(config)

    assert isinstance(runner._executor._reader, PolarsDeltaReader)
    assert isinstance(runner._executor._writer, PolarsDeltaWriter)


def test_from_config_delta_with_tmp_root_builds_temp_store(tmp_path: Any) -> None:
    from loom.etl._runner import ETLRunner
    from loom.etl._storage_config import DeltaConfig
    from loom.etl._temp_store import IntermediateStore

    config = DeltaConfig(root=str(tmp_path), tmp_root=str(tmp_path / "tmp"))
    runner = ETLRunner.from_config(config)

    assert isinstance(runner._temp_store, IntermediateStore)


def test_from_config_delta_without_tmp_root_has_no_temp_store(tmp_path: Any) -> None:
    from loom.etl._runner import ETLRunner
    from loom.etl._storage_config import DeltaConfig

    config = DeltaConfig(root=str(tmp_path))
    runner = ETLRunner.from_config(config)

    assert runner._temp_store is None


def test_from_config_delta_replace_evolves_config(tmp_path: Any) -> None:
    """msgspec.structs.replace produces a working independent config."""
    import msgspec

    from loom.etl._runner import ETLRunner
    from loom.etl._storage_config import DeltaConfig
    from loom.etl.backends.polars._reader import PolarsDeltaReader

    base = DeltaConfig(root=str(tmp_path))
    other = msgspec.structs.replace(base, root=str(tmp_path / "other"))

    runner = ETLRunner.from_config(other)
    assert isinstance(runner._executor._reader, PolarsDeltaReader)
    assert base.root != other.root


# ---------------------------------------------------------------------------
# ETLRunner.from_dict — programmatic dict entry point
# ---------------------------------------------------------------------------


def test_from_dict_delta_builds_polars_backends(tmp_path: Any) -> None:
    from loom.etl._runner import ETLRunner
    from loom.etl.backends.polars._reader import PolarsDeltaReader

    runner = ETLRunner.from_dict({"root": str(tmp_path)})
    assert isinstance(runner._executor._reader, PolarsDeltaReader)


def test_from_dict_unity_catalog_without_spark_raises() -> None:
    from loom.etl._runner import ETLRunner

    with pytest.raises(ValueError, match="SparkSession"):
        ETLRunner.from_dict({"type": "unity_catalog"})


def test_from_dict_unknown_type_raises() -> None:
    from loom.etl._runner import ETLRunner

    with pytest.raises(ValueError, match="Unknown storage backend"):
        ETLRunner.from_dict({"type": "unknown_backend"})


def test_from_dict_invalid_storage_shape_raises() -> None:
    import msgspec

    from loom.etl._runner import ETLRunner

    with pytest.raises(msgspec.ValidationError):
        ETLRunner.from_dict({"root": 123})  # root must be str


def test_from_dict_with_observability_dict(tmp_path: Any) -> None:
    from loom.etl._runner import ETLRunner

    runner = ETLRunner.from_dict(
        {"root": str(tmp_path)},
        observability={"log": False},
    )
    assert runner is not None


# ---------------------------------------------------------------------------
# ETLRunner — cleaner injection
# ---------------------------------------------------------------------------


def test_from_config_with_injected_cleaner(tmp_path: Any) -> None:
    from loom.etl._runner import ETLRunner
    from loom.etl._storage_config import DeltaConfig

    deleted: list[str] = []

    class SpyCleaner:
        def delete_tree(self, path: str) -> None:
            deleted.append(path)

    config = DeltaConfig(root=str(tmp_path), tmp_root=str(tmp_path / "tmp"))
    runner = ETLRunner.from_config(config, cleaner=SpyCleaner())

    assert runner._temp_store is not None
    runner._temp_store.cleanup_run("run-xyz")
    assert any("run-xyz" in p for p in deleted)
