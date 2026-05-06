"""Tests for ETLExecutor run_step, run_process and run_pipeline."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from typing import Any

import pytest

from loom.core.observability.runtime import ObservabilityRuntime
from loom.etl import ETLParams, ETLPipeline, ETLProcess, ETLStep, FromTable, IntoTable
from loom.etl.compiler import ETLCompiler
from loom.etl.executor import ETLExecutor, ThreadDispatcher
from loom.etl.lineage._records import (
    EventName as LineageEventName,
)
from loom.etl.lineage._records import (
    RunContext as LineageRunContext,
)
from loom.etl.lineage._records import (
    RunStatus as LineageRunStatus,
)
from loom.etl.testing import StubRunObserver, StubSourceReader, StubTargetWriter

ExecutorFactory = Callable[..., tuple[ETLExecutor, StubTargetWriter, StubRunObserver]]


def _table_ref(spec: Any) -> Any:
    table_ref = getattr(spec, "table_ref", None)
    assert table_ref is not None
    return table_ref


class RunParams(ETLParams):  # type: ignore[misc]
    run_date: date


SENTINEL_A = object()
SENTINEL_B = object()


class StepA(ETLStep[RunParams]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.a").replace()

    def execute(self, params: RunParams, *, orders: Any) -> Any:  # type: ignore[override]
        return orders


class StepB(ETLStep[RunParams]):
    customers = FromTable("raw.customers")
    target = IntoTable("staging.b").replace()

    def execute(self, params: RunParams, *, customers: Any) -> Any:  # type: ignore[override]
        return customers


class StepNoSources(ETLStep[RunParams]):
    target = IntoTable("staging.calendar").replace()

    def execute(self, params: RunParams) -> Any:  # type: ignore[override]
        return {"generated": True}


class StepStreaming(ETLStep[RunParams]):
    streaming = True
    orders = FromTable("raw.orders")
    target = IntoTable("staging.stream").replace()

    def execute(self, params: RunParams, *, orders: Any) -> Any:  # type: ignore[override]
        return orders


class FailingStep(ETLStep[RunParams]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.fail").replace()

    def execute(self, params: RunParams, *, orders: Any) -> Any:  # type: ignore[override]
        raise ValueError("intentional failure")


class ProcAB(ETLProcess[RunParams]):
    steps = [StepA, StepB]


class ProcParallel(ETLProcess[RunParams]):
    steps = [[StepA, StepB]]


class PipelineSeq(ETLPipeline[RunParams]):
    processes = [ProcAB]


class PipelineParallel(ETLPipeline[RunParams]):
    processes = [[ProcAB]]


@pytest.fixture
def params() -> RunParams:
    return RunParams(run_date=date(2024, 1, 5))


@pytest.fixture
def compiler() -> ETLCompiler:
    return ETLCompiler()


@pytest.fixture
def executor_factory() -> ExecutorFactory:
    def _make(
        frames: dict[str, Any] | None = None,
        observer: StubRunObserver | None = None,
        dispatcher: Any = None,
    ) -> tuple[ETLExecutor, StubTargetWriter, StubRunObserver]:
        writer = StubTargetWriter()
        run_observer = observer or StubRunObserver()
        reader = StubSourceReader(frames or {"orders": SENTINEL_A, "customers": SENTINEL_B})
        executor = ETLExecutor(
            reader,
            writer,
            observability=ObservabilityRuntime([run_observer]),
            dispatcher=dispatcher,
        )
        return executor, writer, run_observer

    return _make


class TestRunStep:
    @pytest.mark.parametrize(
        "step_type,expected_frame,expected_target",
        [
            (StepA, SENTINEL_A, "staging.a"),
            (StepB, SENTINEL_B, "staging.b"),
            (StepNoSources, {"generated": True}, "staging.calendar"),
        ],
    )
    def test_run_step_writes_expected_output(
        self,
        compiler: ETLCompiler,
        params: RunParams,
        executor_factory: ExecutorFactory,
        step_type: type[ETLStep[RunParams]],
        expected_frame: object,
        expected_target: str,
    ) -> None:
        plan = compiler.compile_step(step_type)
        executor, writer, _ = executor_factory()
        executor.run_step(plan, params)
        assert len(writer.written) == 1
        frame, spec = writer.written[0]
        assert frame == expected_frame
        table_ref = _table_ref(spec)
        assert table_ref.ref == expected_target

    def test_run_step_observer_success_and_metadata(
        self,
        compiler: ETLCompiler,
        params: RunParams,
        executor_factory: ExecutorFactory,
    ) -> None:
        plan = compiler.compile_step(StepA)
        executor, _, observer = executor_factory()
        context = LineageRunContext(run_id="fixed-run-id")
        executor.run_step(plan, params, context)

        assert observer.event_names == [LineageEventName.STEP_START, LineageEventName.STEP_END]
        assert observer.step_statuses == [LineageRunStatus.SUCCESS]
        start_event = next(data for name, data in observer.events if name == "step_start")
        assert start_event["step"] == "StepA"
        assert start_event["run_id"] == "fixed-run-id"

    def test_run_step_forwards_streaming_flag_to_writer(
        self,
        compiler: ETLCompiler,
        params: RunParams,
        executor_factory: ExecutorFactory,
    ) -> None:
        streaming_plan = compiler.compile_step(StepStreaming)
        normal_plan = compiler.compile_step(StepA)
        executor, writer, _ = executor_factory()

        executor.run_step(streaming_plan, params)
        executor.run_step(normal_plan, params)

        assert writer.streaming_flags == [True, False]

    def test_run_step_error_events_and_order(
        self,
        compiler: ETLCompiler,
        params: RunParams,
        executor_factory: ExecutorFactory,
    ) -> None:
        plan = compiler.compile_step(FailingStep)
        executor, _, observer = executor_factory()
        with pytest.raises(ValueError, match="intentional failure"):
            executor.run_step(plan, params)

        assert LineageEventName.STEP_ERROR in observer.event_names
        assert observer.step_statuses == [LineageRunStatus.FAILED]
        names = observer.event_names
        assert names.index(LineageEventName.STEP_ERROR) < names.index(LineageEventName.STEP_END)


class TestRunProcess:
    @pytest.mark.parametrize(
        "process_type,expect_order",
        [
            (ProcAB, True),
            (ProcParallel, False),
        ],
    )
    def test_run_process_writes_steps(
        self,
        compiler: ETLCompiler,
        params: RunParams,
        executor_factory: ExecutorFactory,
        process_type: type[ETLProcess[RunParams]],
        expect_order: bool,
    ) -> None:
        plan = compiler.compile_process(process_type)
        executor, writer, _ = executor_factory()
        executor.run_process(plan, params)

        assert len(writer.written) == 2
        refs: list[str] = []
        for _, spec in writer.written:
            table_ref = getattr(spec, "table_ref", None)
            if table_ref is not None:
                refs.append(table_ref.ref)
        assert set(refs) == {"staging.a", "staging.b"}
        if expect_order:
            assert refs == ["staging.a", "staging.b"]

    def test_run_process_observer_success_events(
        self,
        compiler: ETLCompiler,
        params: RunParams,
        executor_factory: ExecutorFactory,
    ) -> None:
        plan = compiler.compile_process(ProcAB)
        executor, _, observer = executor_factory()
        executor.run_process(plan, params)
        assert LineageEventName.PROCESS_START in observer.event_names
        assert LineageEventName.PROCESS_END in observer.event_names
        assert observer.step_statuses == [LineageRunStatus.SUCCESS, LineageRunStatus.SUCCESS]

    def test_run_process_failed_status_on_step_error(
        self,
        compiler: ETLCompiler,
        params: RunParams,
        executor_factory: ExecutorFactory,
    ) -> None:
        class ProcFail(ETLProcess[RunParams]):
            steps = [FailingStep]

        plan = compiler.compile_process(ProcFail)
        executor, _, observer = executor_factory()
        with pytest.raises(ValueError):
            executor.run_process(plan, params)

        process_end = next(data for name, data in observer.events if name == "process_end")
        assert process_end["status"] == "failed"


class TestRunPipeline:
    @pytest.mark.parametrize(
        "pipeline_type",
        [PipelineSeq, PipelineParallel],
    )
    def test_run_pipeline_writes_expected_rows(
        self,
        compiler: ETLCompiler,
        params: RunParams,
        executor_factory: ExecutorFactory,
        pipeline_type: type[ETLPipeline[RunParams]],
    ) -> None:
        plan = compiler.compile(pipeline_type)
        executor, writer, _ = executor_factory()
        executor.run_pipeline(plan, params)
        assert len(writer.written) == 2

    def test_run_pipeline_observer_start_end_and_success(
        self,
        compiler: ETLCompiler,
        params: RunParams,
        executor_factory: ExecutorFactory,
    ) -> None:
        plan = compiler.compile(PipelineSeq)
        executor, _, observer = executor_factory()
        executor.run_pipeline(plan, params)
        assert LineageEventName.PIPELINE_START in observer.event_names
        assert LineageEventName.PIPELINE_END in observer.event_names
        assert observer.pipeline_statuses == [LineageRunStatus.SUCCESS]

    def test_run_pipeline_failed_status_on_step_error(
        self,
        compiler: ETLCompiler,
        params: RunParams,
        executor_factory: ExecutorFactory,
    ) -> None:
        class ProcFail(ETLProcess[RunParams]):
            steps = [FailingStep]

        class PipelineFail(ETLPipeline[RunParams]):
            processes = [ProcFail]

        plan = compiler.compile(PipelineFail)
        executor, _, observer = executor_factory()
        with pytest.raises(ValueError):
            executor.run_pipeline(plan, params)
        assert observer.pipeline_statuses == [LineageRunStatus.FAILED]


class TestThreadDispatcher:
    @pytest.mark.parametrize(
        "max_workers,n",
        [(None, 0), (None, 5), (2, 4)],
    )
    def test_thread_dispatcher_runs_all_tasks(self, max_workers: int | None, n: int) -> None:
        results: list[int] = []
        kwargs = {"max_workers": max_workers} if max_workers is not None else {}
        dispatcher = ThreadDispatcher(**kwargs)

        def make_task(value: int) -> Callable[[], None]:
            def task() -> None:
                results.append(value)

            return task

        tasks = [make_task(i) for i in range(n)]
        dispatcher.run_all(tasks)
        assert sorted(results) == list(range(n))

    def test_thread_dispatcher_reraises_first_exception(self) -> None:
        def fail() -> None:
            raise RuntimeError("thread error")

        dispatcher = ThreadDispatcher()
        with pytest.raises(RuntimeError, match="thread error"):
            dispatcher.run_all([fail, fail])
