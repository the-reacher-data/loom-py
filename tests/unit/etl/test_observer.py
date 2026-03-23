"""Tests for the observer protocol, StructlogRunObserver, and RunSinkObserver."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

from loom.etl.executor import (
    ETLRunObserver,
    EventName,
    PipelineRunRecord,
    RunSinkObserver,
    RunStatus,
    StepRunRecord,
    StructlogRunObserver,
)
from loom.etl.testing import StubRunObserver

# ---------------------------------------------------------------------------
# RunStatus
# ---------------------------------------------------------------------------


def test_run_status_values() -> None:
    assert RunStatus.SUCCESS == "success"
    assert RunStatus.FAILED == "failed"


# ---------------------------------------------------------------------------
# ETLRunObserver protocol
# ---------------------------------------------------------------------------


def test_structlog_observer_satisfies_protocol() -> None:
    assert isinstance(StructlogRunObserver(), ETLRunObserver)


def test_run_sink_observer_satisfies_protocol() -> None:
    class _NullSink:
        def write(self, record: Any) -> None:
            """Null sink: intentionally discards all records."""

    assert isinstance(RunSinkObserver(_NullSink()), ETLRunObserver)


def test_stub_observer_satisfies_protocol() -> None:
    assert isinstance(StubRunObserver(), ETLRunObserver)


# ---------------------------------------------------------------------------
# StubRunObserver
# ---------------------------------------------------------------------------


def test_stub_observer_captures_events() -> None:
    obs = StubRunObserver()
    obs.on_step_start(type("Plan", (), {"step_type": type("Step", (), {})})(), "r", "s")
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
    obs.on_pipeline_end("r1", RunStatus.SUCCESS, 100)
    obs.on_pipeline_end("r2", RunStatus.FAILED, 50)
    assert obs.pipeline_statuses == [RunStatus.SUCCESS, RunStatus.FAILED]


def test_stub_observer_empty_on_init() -> None:
    obs = StubRunObserver()
    assert obs.events == []
    assert obs.event_names == []
    assert obs.step_statuses == []


# ---------------------------------------------------------------------------
# StructlogRunObserver
# ---------------------------------------------------------------------------


def test_structlog_observer_emits_step_start() -> None:
    obs = StructlogRunObserver()

    class FakePlan:
        step_type = type("MyStep", (), {})

    with patch("loom.etl.executor.observer._structlog._log") as mock_log:
        obs.on_step_start(FakePlan(), "run-1", "step-1")
        mock_log.info.assert_called_once()
        call_args = mock_log.info.call_args
        assert call_args[0][0] == EventName.STEP_START
        assert call_args[1]["step"] == "MyStep"
        assert call_args[1]["run_id"] == "run-1"
        assert call_args[1]["step_run_id"] == "step-1"


def test_structlog_observer_emits_step_error_at_error_level() -> None:
    obs = StructlogRunObserver()
    exc = ValueError("bad input")

    with patch("loom.etl.executor.observer._structlog._log") as mock_log:
        obs.on_step_error("step-1", exc)
        mock_log.error.assert_called_once()
        call_args = mock_log.error.call_args
        assert call_args[0][0] == EventName.STEP_ERROR
        assert call_args[1]["step_run_id"] == "step-1"
        assert "ValueError" in call_args[1]["error"]


def test_structlog_observer_emits_step_end() -> None:
    obs = StructlogRunObserver()

    with patch("loom.etl.executor.observer._structlog._log") as mock_log:
        obs.on_step_end("step-1", RunStatus.SUCCESS, 42)
        mock_log.info.assert_called_once()
        call_args = mock_log.info.call_args
        assert call_args[0][0] == EventName.STEP_END
        assert call_args[1]["status"] == RunStatus.SUCCESS
        assert call_args[1]["duration_ms"] == 42


# ---------------------------------------------------------------------------
# RunSinkObserver
# ---------------------------------------------------------------------------


class _CapturingSink:
    """In-test sink that records every write call."""

    def __init__(self) -> None:
        self.records: list[Any] = []

    def write(self, record: Any) -> None:
        self.records.append(record)


def test_run_sink_observer_writes_step_record_on_end() -> None:
    sink = _CapturingSink()
    obs = RunSinkObserver(sink)

    class FakePlan:
        step_type = type("BuildOrders", (), {})

    obs.on_step_start(FakePlan(), "run-1", "step-1")
    obs.on_step_end("step-1", RunStatus.SUCCESS, 100)

    assert len(sink.records) == 1
    record = sink.records[0]
    assert isinstance(record, StepRunRecord)
    assert record.run_id == "run-1"
    assert record.step == "BuildOrders"
    assert record.status == RunStatus.SUCCESS
    assert record.duration_ms == 100
    assert record.error is None


def test_run_sink_observer_captures_error_in_step_record() -> None:
    sink = _CapturingSink()
    obs = RunSinkObserver(sink)

    class FakePlan:
        step_type = type("FailingStep", (), {})

    obs.on_step_start(FakePlan(), "run-1", "step-1")
    obs.on_step_error("step-1", RuntimeError("disk full"))
    obs.on_step_end("step-1", RunStatus.FAILED, 50)

    assert len(sink.records) == 1
    record = sink.records[0]
    assert record.status == RunStatus.FAILED
    assert "disk full" in record.error


def test_run_sink_observer_writes_pipeline_record_on_end() -> None:
    sink = _CapturingSink()
    obs = RunSinkObserver(sink)

    class FakePlan:
        pipeline_type = type("DailyPipeline", (), {})

    obs.on_pipeline_start(FakePlan(), object(), "run-1")
    obs.on_pipeline_end("run-1", RunStatus.SUCCESS, 300)

    assert len(sink.records) == 1
    record = sink.records[0]
    assert isinstance(record, PipelineRunRecord)
    assert record.pipeline == "DailyPipeline"
    assert record.status == RunStatus.SUCCESS


def test_run_sink_observer_does_not_write_on_start_events() -> None:
    sink = _CapturingSink()
    obs = RunSinkObserver(sink)

    class FakePlan:
        step_type = type("MyStep", (), {})

    obs.on_step_start(FakePlan(), "run-1", "step-1")
    assert sink.records == []
