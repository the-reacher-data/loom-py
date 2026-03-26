"""Tests for ETLExecutor — run_step, run_process, run_pipeline."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from loom.etl import ETLParams, ETLPipeline, ETLProcess, ETLStep, FromTable, IntoTable
from loom.etl.compiler import ETLCompiler
from loom.etl.executor import ETLExecutor, EventName, RunContext, RunStatus, ThreadDispatcher
from loom.etl.testing import StubRunObserver, StubSourceReader, StubTargetWriter


class RunParams(ETLParams):
    run_date: date


SENTINEL_A = object()
SENTINEL_B = object()


class StepA(ETLStep[RunParams]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.a").replace()

    def execute(self, params: RunParams, *, orders: Any) -> Any:
        return orders


class StepB(ETLStep[RunParams]):
    customers = FromTable("raw.customers")
    target = IntoTable("staging.b").replace()

    def execute(self, params: RunParams, *, customers: Any) -> Any:
        return customers


class StepNoSources(ETLStep[RunParams]):
    target = IntoTable("staging.calendar").replace()

    def execute(self, params: RunParams) -> Any:
        return {"generated": True}


class ProcAB(ETLProcess[RunParams]):
    steps = [StepA, StepB]


class ProcParallel(ETLProcess[RunParams]):
    steps = [[StepA, StepB]]


class PipelineSeq(ETLPipeline[RunParams]):
    processes = [ProcAB]


class PipelineParallel(ETLPipeline[RunParams]):
    processes = [[ProcAB]]


_PARAMS = RunParams(run_date=date(2024, 1, 5))
_COMPILER = ETLCompiler()


def _executor(
    frames: dict[str, Any] | None = None,
    observer: StubRunObserver | None = None,
    dispatcher: Any = None,
) -> tuple[ETLExecutor, StubTargetWriter, StubRunObserver]:
    writer = StubTargetWriter()
    obs = observer or StubRunObserver()
    reader = StubSourceReader(frames or {"orders": SENTINEL_A, "customers": SENTINEL_B})
    exc = ETLExecutor(reader, writer, observers=[obs], dispatcher=dispatcher)
    return exc, writer, obs


def test_run_step_reads_source_and_writes_target() -> None:
    plan = _COMPILER.compile_step(StepA)
    exc, writer, _ = _executor()
    exc.run_step(plan, _PARAMS)
    assert len(writer.written) == 1
    frame, spec = writer.written[0]
    assert frame is SENTINEL_A
    assert spec.table_ref is not None
    assert spec.table_ref.ref == "staging.a"


def test_run_step_no_sources_writes_target() -> None:
    plan = _COMPILER.compile_step(StepNoSources)
    exc, writer, _ = _executor()
    exc.run_step(plan, _PARAMS)
    assert len(writer.written) == 1
    assert writer.written[0][0] == {"generated": True}


def test_run_step_multiple_sources_passes_all_frames() -> None:
    plan = _COMPILER.compile_step(StepB)
    exc, writer, _ = _executor()
    exc.run_step(plan, _PARAMS)
    frame, _ = writer.written[0]
    assert frame is SENTINEL_B


def test_run_step_observer_step_start_and_end() -> None:
    plan = _COMPILER.compile_step(StepA)
    exc, _, obs = _executor()
    exc.run_step(plan, _PARAMS)
    assert obs.event_names == [EventName.STEP_START, EventName.STEP_END]
    assert obs.step_statuses == [RunStatus.SUCCESS]


def test_run_step_observer_receives_step_name() -> None:
    plan = _COMPILER.compile_step(StepA)
    exc, _, obs = _executor()
    exc.run_step(plan, _PARAMS)
    start_event = next(d for name, d in obs.events if name == "step_start")
    assert start_event["step"] == "StepA"


def test_run_step_run_id_consistent_across_events() -> None:
    plan = _COMPILER.compile_step(StepA)
    exc, _, obs = _executor()
    ctx = RunContext(run_id="fixed-run-id")
    exc.run_step(plan, _PARAMS, ctx)
    start_event = next(d for name, d in obs.events if name == "step_start")
    assert start_event["run_id"] == "fixed-run-id"


class FailingStep(ETLStep[RunParams]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.fail").replace()

    def execute(self, params: RunParams, *, orders: Any) -> Any:
        raise ValueError("intentional failure")


def test_run_step_reraises_on_execute_error() -> None:
    plan = _COMPILER.compile_step(FailingStep)
    exc, _, _ = _executor()
    with pytest.raises(ValueError, match="intentional failure"):
        exc.run_step(plan, _PARAMS)


def test_run_step_observer_called_on_error() -> None:
    plan = _COMPILER.compile_step(FailingStep)
    exc, _, obs = _executor()
    with pytest.raises(ValueError):
        exc.run_step(plan, _PARAMS)
    assert EventName.STEP_ERROR in obs.event_names
    assert obs.step_statuses == [RunStatus.FAILED]


def test_run_step_observer_error_before_end() -> None:
    plan = _COMPILER.compile_step(FailingStep)
    exc, _, obs = _executor()
    with pytest.raises(ValueError):
        exc.run_step(plan, _PARAMS)
    names = obs.event_names
    assert names.index(EventName.STEP_ERROR) < names.index(EventName.STEP_END)


def test_run_process_sequential_steps_in_order() -> None:
    plan = _COMPILER.compile_process(ProcAB)
    exc, writer, _ = _executor()
    exc.run_process(plan, _PARAMS)
    assert len(writer.written) == 2
    assert writer.written[0][1].table_ref.ref == "staging.a"  # type: ignore[union-attr]
    assert writer.written[1][1].table_ref.ref == "staging.b"  # type: ignore[union-attr]


def test_run_process_observer_events() -> None:
    plan = _COMPILER.compile_process(ProcAB)
    exc, _, obs = _executor()
    exc.run_process(plan, _PARAMS)
    assert EventName.PROCESS_START in obs.event_names
    assert EventName.PROCESS_END in obs.event_names
    assert obs.step_statuses == [RunStatus.SUCCESS, RunStatus.SUCCESS]


def test_run_process_parallel_dispatches_both_steps() -> None:
    plan = _COMPILER.compile_process(ProcParallel)
    exc, writer, _ = _executor()
    exc.run_process(plan, _PARAMS)
    assert len(writer.written) == 2


def test_run_process_observer_failed_on_step_error() -> None:
    class ProcFail(ETLProcess[RunParams]):
        steps = [FailingStep]

    plan = _COMPILER.compile_process(ProcFail)
    exc, _, obs = _executor()
    with pytest.raises(ValueError):
        exc.run_process(plan, _PARAMS)
    proc_end = next(d for name, d in obs.events if name == "process_end")
    assert proc_end["status"] == "failed"


def test_run_pipeline_sequential_processes() -> None:
    plan = _COMPILER.compile(PipelineSeq)
    exc, writer, obs = _executor()
    exc.run_pipeline(plan, _PARAMS)
    assert len(writer.written) == 2
    assert obs.pipeline_statuses == [RunStatus.SUCCESS]


def test_run_pipeline_parallel_processes_dispatched() -> None:
    plan = _COMPILER.compile(PipelineParallel)
    exc, writer, _ = _executor()
    exc.run_pipeline(plan, _PARAMS)
    assert len(writer.written) == 2


def test_run_pipeline_observer_pipeline_start_end() -> None:
    plan = _COMPILER.compile(PipelineSeq)
    exc, _, obs = _executor()
    exc.run_pipeline(plan, _PARAMS)
    assert EventName.PIPELINE_START in obs.event_names
    assert EventName.PIPELINE_END in obs.event_names


def test_run_pipeline_failed_status_on_step_error() -> None:
    class ProcFail(ETLProcess[RunParams]):
        steps = [FailingStep]

    class PipelineFail(ETLPipeline[RunParams]):
        processes = [ProcFail]

    plan = _COMPILER.compile(PipelineFail)
    exc, _, obs = _executor()
    with pytest.raises(ValueError):
        exc.run_pipeline(plan, _PARAMS)
    assert obs.pipeline_statuses == [RunStatus.FAILED]


@pytest.mark.parametrize(
    "max_workers,n",
    [(None, 0), (None, 5), (2, 4)],
)
def test_thread_dispatcher_runs_all_tasks(max_workers: int | None, n: int) -> None:
    results: list[int] = []
    kwargs = {"max_workers": max_workers} if max_workers is not None else {}
    dispatcher = ThreadDispatcher(**kwargs)
    dispatcher.run_all([lambda i=i: results.append(i) for i in range(n)])
    assert sorted(results) == list(range(n))


def test_thread_dispatcher_reraises_first_exception() -> None:
    def fail() -> None:
        raise RuntimeError("thread error")

    dispatcher = ThreadDispatcher()
    with pytest.raises(RuntimeError, match="thread error"):
        dispatcher.run_all([fail, fail])
