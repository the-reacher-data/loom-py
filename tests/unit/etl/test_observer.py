"""Tests for the observer protocol, StructlogRunObserver, and ExecutionRecordsObserver."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast
from unittest.mock import patch

import pytest

from loom.etl.executor import (
    ETLRunObserver,
    EventName,
    ExecutionRecordsObserver,
    NoopRunObserver,
    PipelineRunRecord,
    RunContext,
    RunStatus,
    StepRunRecord,
    StructlogRunObserver,
)
from loom.etl.observability.recording._recorder import (
    _build_pipeline_record,
    _build_process_record,
    _EntityContext,
    _FailureContext,
)
from loom.etl.testing import StubRunObserver


class _NullSink:
    def write_record(self, record: Any) -> None:
        """Null sink: intentionally discards all records."""


class _CapturingSink:
    """In-test sink that records every write call."""

    def __init__(self) -> None:
        self.records: list[Any] = []

    def write_record(self, record: Any) -> None:
        self.records.append(record)


def test_run_status_values() -> None:
    assert RunStatus.SUCCESS == "success"
    assert RunStatus.FAILED == "failed"


@pytest.mark.parametrize(
    "observer",
    [
        NoopRunObserver(),
        StructlogRunObserver(),
        ExecutionRecordsObserver(_NullSink()),
        StubRunObserver(),
    ],
)
def test_observer_satisfies_protocol(observer: ETLRunObserver) -> None:
    assert isinstance(observer, ETLRunObserver)


def test_stub_observer_captures_events() -> None:
    obs = StubRunObserver()
    obs.on_step_start(type("Plan", (), {"step_type": type("Step", (), {})})(), RunContext("r"), "s")
    obs.on_step_end("s", RunStatus.SUCCESS, 5)
    assert EventName.STEP_START in obs.event_names
    assert EventName.STEP_END in obs.event_names


def test_stub_observer_step_statuses() -> None:
    obs = StubRunObserver()
    obs.on_step_end("s1", RunStatus.SUCCESS, 5)
    obs.on_step_end("s2", RunStatus.FAILED, 2)
    assert obs.step_statuses == [RunStatus.SUCCESS, RunStatus.FAILED]


def test_stub_observer_pipeline_statuses() -> None:
    obs = StubRunObserver()
    obs.on_pipeline_end(RunContext("r1"), RunStatus.SUCCESS, 100)
    obs.on_pipeline_end(RunContext("r2"), RunStatus.FAILED, 50)
    assert obs.pipeline_statuses == [RunStatus.SUCCESS, RunStatus.FAILED]


def test_stub_observer_empty_on_init() -> None:
    obs = StubRunObserver()
    assert obs.events == []
    assert obs.event_names == []
    assert obs.step_statuses == []


def test_noop_observer_has_no_side_effects() -> None:
    obs = NoopRunObserver()

    class _PipelinePlan:
        pipeline_type = type("Pipe", (), {})

    class _ProcessPlan:
        process_type = type("Proc", (), {})
        nodes = ()

    class _StepPlan:
        step_type = type("Step", (), {})
        source_bindings = ()
        target_binding = type("TB", (), {"spec": object()})()
        backend = "polars"

    ctx = RunContext("run-1")
    obs.on_pipeline_start(cast(Any, _PipelinePlan()), object(), ctx)
    obs.on_process_start(cast(Any, _ProcessPlan()), ctx, "process-1")
    obs.on_step_start(cast(Any, _StepPlan()), ctx, "step-1")
    obs.on_step_error("step-1", RuntimeError("boom"))
    obs.on_step_end("step-1", RunStatus.SUCCESS, 1)
    obs.on_process_end("process-1", RunStatus.SUCCESS, 2)
    obs.on_pipeline_end(ctx, RunStatus.SUCCESS, 3)


def test_structlog_observer_emits_step_start() -> None:
    obs = StructlogRunObserver()

    class _Spec:
        table_ref = None
        temp_name = None
        path = "s3://raw/data"
        mode = "replace"

    class _Binding:
        spec = _Spec()
        alias = "data"

    class _FakePlan:
        step_type = type("MyStep", (), {})
        source_bindings = (_Binding(),)
        target_binding = type("TB", (), {"spec": _Spec()})()
        backend = "polars"

    with patch("loom.etl.observability.observers.structlog._log") as mock_log:
        obs.on_step_start(_FakePlan(), RunContext("run-1"), "step-1")
        mock_log.info.assert_called_once()
        call_args = mock_log.info.call_args
        assert call_args[0][0] == EventName.STEP_START
        assert call_args[1]["step"] == "MyStep"
        assert call_args[1]["run_id"] == "run-1"
        assert call_args[1]["step_run_id"] == "step-1"
        assert "sources" in call_args[1]
        assert "target" in call_args[1]
        assert "write_mode" in call_args[1]


def test_structlog_observer_emits_step_error_at_error_level() -> None:
    obs = StructlogRunObserver()
    exc = ValueError("bad input")

    with patch("loom.etl.observability.observers.structlog._log") as mock_log:
        obs.on_step_error("step-1", exc)
        mock_log.error.assert_called_once()
        call_args = mock_log.error.call_args
        assert call_args[0][0] == EventName.STEP_ERROR
        assert call_args[1]["step_run_id"] == "step-1"
        assert "ValueError" in call_args[1]["error"]


def test_structlog_observer_emits_step_end() -> None:
    obs = StructlogRunObserver()

    with patch("loom.etl.observability.observers.structlog._log") as mock_log:
        obs.on_step_end("step-1", RunStatus.SUCCESS, 42)
        mock_log.info.assert_called_once()
        call_args = mock_log.info.call_args
        assert call_args[0][0] == EventName.STEP_END
        assert call_args[1]["status"] == RunStatus.SUCCESS
        assert call_args[1]["duration_ms"] == 42


def test_execution_records_observer_writes_step_record_and_not_on_start() -> None:
    sink = _CapturingSink()
    obs = ExecutionRecordsObserver(sink)

    class FakePlan:
        step_type = type("BuildOrders", (), {})

    obs.on_step_start(FakePlan(), RunContext("run-1"), "step-1")
    assert sink.records == []

    obs.on_step_end("step-1", RunStatus.SUCCESS, 100)
    assert len(sink.records) == 1
    [record] = sink.records
    assert isinstance(record, StepRunRecord)
    assert record.run_id == "run-1"
    assert record.step == "BuildOrders"
    assert record.status == RunStatus.SUCCESS
    assert record.duration_ms == 100
    assert record.error is None


def test_execution_records_observer_captures_error_in_step_record() -> None:
    sink = _CapturingSink()
    obs = ExecutionRecordsObserver(sink)

    class FakePlan:
        step_type = type("FailingStep", (), {})

    obs.on_step_start(FakePlan(), RunContext("run-1"), "step-1")
    obs.on_step_error("step-1", RuntimeError("disk full"))
    obs.on_step_end("step-1", RunStatus.FAILED, 50)

    assert len(sink.records) == 1
    [record] = sink.records
    assert record.status == RunStatus.FAILED
    assert "disk full" in record.error
    assert record.error_type == "RuntimeError"
    assert record.error_message == "disk full"


def test_execution_records_observer_propagates_failure_to_process_and_pipeline() -> None:
    sink = _CapturingSink()
    obs = ExecutionRecordsObserver(sink)

    class _PipePlan:
        pipeline_type = type("DailyPipeline", (), {})

    class _ProcPlan:
        process_type = type("PreparedProcess", (), {})

    class _StepPlan:
        step_type = type("FailingStep", (), {})

    ctx = RunContext("run-1")
    obs.on_pipeline_start(_PipePlan(), object(), ctx)
    obs.on_process_start(_ProcPlan(), ctx, "proc-1")
    obs.on_step_start(_StepPlan(), RunContext("run-1", process_run_id="proc-1"), "step-1")
    obs.on_step_error("step-1", ValueError("bad cast"))
    obs.on_step_end("step-1", RunStatus.FAILED, 7)
    obs.on_process_end("proc-1", RunStatus.FAILED, 8)
    obs.on_pipeline_end(ctx, RunStatus.FAILED, 9)

    assert len(sink.records) >= 2
    process_record, pipeline_record = sink.records[-2:]

    assert process_record.status == RunStatus.FAILED
    assert process_record.failed_step == "FailingStep"
    assert process_record.failed_step_run_id == "step-1"
    assert process_record.error_type == "ValueError"
    assert process_record.error_message == "bad cast"

    assert pipeline_record.status == RunStatus.FAILED
    assert pipeline_record.failed_step == "FailingStep"
    assert pipeline_record.failed_step_run_id == "step-1"
    assert pipeline_record.error_type == "ValueError"
    assert pipeline_record.error_message == "bad cast"


def test_execution_records_observer_writes_pipeline_record_on_end() -> None:
    sink = _CapturingSink()
    obs = ExecutionRecordsObserver(sink)

    class FakePlan:
        pipeline_type = type("DailyPipeline", (), {})

    ctx = RunContext("run-1")
    obs.on_pipeline_start(FakePlan(), object(), ctx)
    obs.on_pipeline_end(ctx, RunStatus.SUCCESS, 300)

    assert len(sink.records) == 1
    [record] = sink.records
    assert isinstance(record, PipelineRunRecord)
    assert record.pipeline == "DailyPipeline"
    assert record.status == RunStatus.SUCCESS


def test_build_pipeline_record_without_failure() -> None:
    entity_ctx = _EntityContext(
        run_ctx=RunContext("run-1", correlation_id="corr-1", attempt=2),
        name="DailyPipeline",
        started_at=datetime.now(tz=UTC),
    )
    record = _build_pipeline_record(entity_ctx, None, status=RunStatus.SUCCESS, duration_ms=120)

    assert record.run_id == "run-1"
    assert record.pipeline == "DailyPipeline"
    assert record.status is RunStatus.SUCCESS
    assert record.error is None
    assert record.failed_step_run_id is None


def test_build_pipeline_record_with_failure() -> None:
    entity_ctx = _EntityContext(
        run_ctx=RunContext("run-1"),
        name="DailyPipeline",
        started_at=datetime.now(tz=UTC),
    )
    failure = _FailureContext(
        step_run_id="step-1",
        step="BadStep",
        error="ValueError('boom')",
        error_type="ValueError",
        error_message="boom",
    )
    record = _build_pipeline_record(entity_ctx, failure, status=RunStatus.FAILED, duration_ms=5)

    assert record.status is RunStatus.FAILED
    assert record.error_type == "ValueError"
    assert record.failed_step == "BadStep"
    assert record.failed_step_run_id == "step-1"


def test_build_process_record_without_failure() -> None:
    entity_ctx = _EntityContext(
        run_ctx=RunContext("run-1", correlation_id="corr-1", attempt=2),
        name="PreparedProcess",
        started_at=datetime.now(tz=UTC),
    )
    record = _build_process_record(
        entity_ctx,
        "proc-1",
        None,
        status=RunStatus.SUCCESS,
        duration_ms=30,
    )

    assert record.run_id == "run-1"
    assert record.process_run_id == "proc-1"
    assert record.process == "PreparedProcess"
    assert record.error is None


def test_build_process_record_with_failure() -> None:
    entity_ctx = _EntityContext(
        run_ctx=RunContext("run-1"),
        name="PreparedProcess",
        started_at=datetime.now(tz=UTC),
    )
    failure = _FailureContext(
        step_run_id="step-1",
        step="BadStep",
        error="RuntimeError('x')",
        error_type="RuntimeError",
        error_message="x",
    )
    record = _build_process_record(
        entity_ctx,
        "proc-1",
        failure,
        status=RunStatus.FAILED,
        duration_ms=12,
    )

    assert record.status is RunStatus.FAILED
    assert record.error_type == "RuntimeError"
    assert record.failed_step == "BadStep"
