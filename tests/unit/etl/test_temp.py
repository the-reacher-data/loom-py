"""Tests for IntoTemp / FromTemp / TempScope / IntermediateStore."""

from __future__ import annotations

import os
import tempfile
from datetime import date
from typing import Any

import pytest

from loom.etl import (
    ETLParams,
    ETLPipeline,
    ETLProcess,
    ETLStep,
    FromTable,
    FromTemp,
    IntoTable,
    IntoTemp,
    Sources,
    TempScope,
)
from loom.etl._source import SourceKind
from loom.etl._temp_store import IntermediateStore
from loom.etl.compiler import ETLCompilationError, ETLCompiler
from loom.etl.testing import StubCatalog

# ---------------------------------------------------------------------------
# Params fixture
# ---------------------------------------------------------------------------


class P(ETLParams):
    run_date: date


# ---------------------------------------------------------------------------
# IntoTemp / FromTemp — spec construction
# ---------------------------------------------------------------------------


def test_into_temp_default_scope() -> None:
    t = IntoTemp("orders")
    assert t.temp_name == "orders"
    assert t.scope is TempScope.RUN


def test_into_temp_correlation_scope() -> None:
    t = IntoTemp("orders", scope=TempScope.CORRELATION)
    assert t.scope is TempScope.CORRELATION


def test_into_temp_to_spec_carries_name_and_scope() -> None:
    spec = IntoTemp("orders", scope=TempScope.CORRELATION)._to_spec()
    assert spec.temp_name == "orders"
    assert spec.temp_scope is TempScope.CORRELATION


def test_from_temp_to_spec_kind_is_temp() -> None:
    spec = FromTemp("orders")._to_spec("data")
    assert spec.kind is SourceKind.TEMP
    assert spec.temp_name == "orders"
    assert spec.alias == "data"


# ---------------------------------------------------------------------------
# Compiler — structural validation with IntoTemp / FromTemp
# ---------------------------------------------------------------------------


class ProduceStep(ETLStep[P]):
    sources = Sources(raw=FromTable("raw.orders"))
    target = IntoTemp("normalized")

    def execute(self, params: P, *, raw: Any) -> Any:
        return raw


class ConsumeStep(ETLStep[P]):
    normalized = FromTemp("normalized")
    target = IntoTable("mart.summary").replace()

    def execute(self, params: P, *, normalized: Any) -> Any:
        return normalized


class TempProcess(ETLProcess[P]):
    steps = [ProduceStep, ConsumeStep]


class TempPipeline(ETLPipeline[P]):
    processes = [TempProcess]


_CATALOG = StubCatalog(tables={"raw.orders": (), "mart.summary": ()})


def test_compile_step_into_temp_succeeds() -> None:
    plan = ETLCompiler().compile_step(ProduceStep)
    assert plan.target_binding.spec.temp_name == "normalized"


def test_compile_step_from_temp_source_binding() -> None:
    plan = ETLCompiler().compile_step(ConsumeStep)
    assert len(plan.source_bindings) == 1
    b = plan.source_bindings[0]
    assert b.alias == "normalized"
    assert b.spec.kind is SourceKind.TEMP
    assert b.spec.temp_name == "normalized"


def test_compile_pipeline_with_temp_passes() -> None:
    plan = ETLCompiler(_CATALOG).compile(TempPipeline)
    assert plan is not None


def test_compile_pipeline_from_temp_without_prior_into_temp_raises() -> None:
    class OrphanProcess(ETLProcess[P]):
        steps = [ConsumeStep]  # no ProduceStep before it

    class OrphanPipeline(ETLPipeline[P]):
        processes = [OrphanProcess]

    with pytest.raises(ETLCompilationError, match="normalized"):
        ETLCompiler(_CATALOG).compile(OrphanPipeline)


def test_compile_pipeline_parallel_steps_do_not_see_each_others_temps() -> None:
    class ParallelProcess(ETLProcess[P]):
        steps = [[ProduceStep, ConsumeStep]]  # parallel — ConsumeStep cannot see ProduceStep

    class ParallelPipeline(ETLPipeline[P]):
        processes = [ParallelProcess]

    with pytest.raises(ETLCompilationError, match="normalized"):
        ETLCompiler(_CATALOG).compile(ParallelPipeline)


def test_compile_pipeline_sequential_step_sees_prior_parallel_temp() -> None:
    class ProduceA(ETLStep[P]):
        sources = Sources(raw=FromTable("raw.orders"))
        target = IntoTemp("norm_a")

        def execute(self, params: P, *, raw: Any) -> Any:
            return raw

    class ConsumeA(ETLStep[P]):
        norm_a = FromTemp("norm_a")
        target = IntoTable("mart.summary").replace()

        def execute(self, params: P, *, norm_a: Any) -> Any:
            return norm_a

    class Proc(ETLProcess[P]):
        steps = [[ProduceA], ConsumeA]  # parallel group with one step, then sequential

    class Pip(ETLPipeline[P]):
        processes = [Proc]

    ETLCompiler(_CATALOG).compile(Pip)  # must not raise


# ---------------------------------------------------------------------------
# IntermediateStore — local filesystem (no Polars/Spark needed for path logic)
# ---------------------------------------------------------------------------


def test_store_cleanup_run_removes_directory() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = IntermediateStore(tmp_root=tmp)
        run_dir = os.path.join(tmp, "runs", "my-run")
        os.makedirs(run_dir)
        assert os.path.isdir(run_dir)
        store.cleanup_run("my-run")
        assert not os.path.exists(run_dir)


def test_store_cleanup_run_is_noop_when_missing() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = IntermediateStore(tmp_root=tmp)
        store.cleanup_run("does-not-exist")  # must not raise


def test_store_cleanup_correlation_removes_directory() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = IntermediateStore(tmp_root=tmp)
        corr_dir = os.path.join(tmp, "correlations", "job-123")
        os.makedirs(corr_dir)
        store.cleanup_correlation("job-123")
        assert not os.path.exists(corr_dir)


def test_store_cleanup_stale_removes_old_entries() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = IntermediateStore(tmp_root=tmp)
        runs_dir = os.path.join(tmp, "runs")
        old_run = os.path.join(runs_dir, "old-run")
        os.makedirs(old_run)
        # backdate mtime by 2 days
        old_time = os.path.getmtime(old_run) - 2 * 86_400
        os.utime(old_run, (old_time, old_time))
        store.cleanup_stale(older_than_seconds=86_400)
        assert not os.path.exists(old_run)


def test_store_cleanup_stale_keeps_recent_entries() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = IntermediateStore(tmp_root=tmp)
        runs_dir = os.path.join(tmp, "runs")
        new_run = os.path.join(runs_dir, "new-run")
        os.makedirs(new_run)
        store.cleanup_stale(older_than_seconds=86_400)
        assert os.path.isdir(new_run)


def test_store_put_raises_for_unknown_type() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = IntermediateStore(tmp_root=tmp)
        with pytest.raises(TypeError, match="unsupported DataFrame type"):
            store.put(
                "x", run_id="r", correlation_id=None, scope=TempScope.RUN, data={"not": "a frame"}
            )


def test_store_put_correlation_without_correlation_id_raises() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = IntermediateStore(tmp_root=tmp)

        class FakeFrame:
            pass

        with pytest.raises(ValueError, match="correlation_id"):
            store.put(
                "x", run_id="r", correlation_id=None, scope=TempScope.CORRELATION, data=FakeFrame()
            )


def test_store_get_raises_when_missing() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = IntermediateStore(tmp_root=tmp)
        with pytest.raises(FileNotFoundError):
            store.get("missing", run_id="r", correlation_id=None)


# ---------------------------------------------------------------------------
# Runner — cleanup_correlation / cleanup_stale_temps without temp_store raise
# ---------------------------------------------------------------------------


def test_runner_cleanup_correlation_without_temp_store_raises() -> None:
    from loom.etl._runner import ETLRunner
    from loom.etl.testing import StubSourceReader, StubTargetWriter

    runner = ETLRunner(StubSourceReader({}), StubTargetWriter(), StubCatalog(tables={}))
    with pytest.raises(RuntimeError, match="tmp_root"):
        runner.cleanup_correlation("job-123")


def test_runner_cleanup_stale_without_temp_store_raises() -> None:
    from loom.etl._runner import ETLRunner
    from loom.etl.testing import StubSourceReader, StubTargetWriter

    runner = ETLRunner(StubSourceReader({}), StubTargetWriter(), StubCatalog(tables={}))
    with pytest.raises(RuntimeError, match="tmp_root"):
        runner.cleanup_stale_temps()
