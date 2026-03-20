"""Tests for ETLRunObserver implementations."""

from __future__ import annotations

import logging

import pytest

from loom.etl.executor import (
    CompositeRunObserver,
    ETLRunObserver,
    EventName,
    LoggingRunObserver,
    NoopRunObserver,
    RunStatus,
)
from loom.etl.testing import StubRunObserver

# ---------------------------------------------------------------------------
# RunStatus
# ---------------------------------------------------------------------------


def test_run_status_values() -> None:
    assert RunStatus.SUCCESS == "success"
    assert RunStatus.FAILED == "failed"


# ---------------------------------------------------------------------------
# NoopRunObserver
# ---------------------------------------------------------------------------


def test_noop_observer_satisfies_protocol() -> None:
    assert isinstance(NoopRunObserver(), ETLRunObserver)


def test_noop_observer_accepts_all_events_without_error() -> None:
    obs = NoopRunObserver()
    obs.on_pipeline_start(object(), object(), "run-1")
    obs.on_pipeline_end("run-1", RunStatus.SUCCESS, 100)
    obs.on_process_start(object(), "run-1", "proc-1")
    obs.on_process_end("proc-1", RunStatus.SUCCESS, 50)
    obs.on_step_start(object(), "run-1", "step-1")
    obs.on_step_end("step-1", RunStatus.SUCCESS, 0, 0, 10)
    obs.on_step_error("step-1", ValueError("oops"))


# ---------------------------------------------------------------------------
# StubRunObserver
# ---------------------------------------------------------------------------


def test_stub_observer_captures_events() -> None:
    obs = StubRunObserver()
    obs.on_step_start(type("Plan", (), {"step_type": type("Step", (), {})})(), "r", "s")
    obs.on_step_end("s", RunStatus.SUCCESS, 0, 0, 5)
    assert EventName.STEP_START in obs.event_names
    assert EventName.STEP_END in obs.event_names


def test_stub_observer_step_statuses() -> None:
    obs = StubRunObserver()
    obs.on_step_end("s1", RunStatus.SUCCESS, 0, 0, 5)
    obs.on_step_end("s2", RunStatus.FAILED, 0, 0, 2)
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
# LoggingRunObserver
# ---------------------------------------------------------------------------


def test_logging_observer_emits_step_start(caplog: pytest.LogCaptureFixture) -> None:
    obs = LoggingRunObserver()

    OrdersStep = type("OrdersStep", (), {})

    class FakePlan:
        step_type = OrdersStep

    with caplog.at_level(logging.INFO, logger="loom.etl"):
        obs.on_step_start(FakePlan(), "run-1", "step-1")

    assert any("step_start" in r.message for r in caplog.records)
    assert any("OrdersStep" in r.message for r in caplog.records)


def test_logging_observer_emits_step_error(caplog: pytest.LogCaptureFixture) -> None:
    obs = LoggingRunObserver()
    with caplog.at_level(logging.ERROR, logger="loom.etl"):
        obs.on_step_error("step-1", ValueError("bad"))

    assert any(r.levelno == logging.ERROR for r in caplog.records)
    assert any("step_error" in r.message for r in caplog.records)


def test_logging_observer_accepts_custom_logger() -> None:
    custom = logging.getLogger("custom.etl")
    obs = LoggingRunObserver(logger=custom)
    assert obs._log is custom


# ---------------------------------------------------------------------------
# CompositeRunObserver
# ---------------------------------------------------------------------------


def test_composite_fans_out_to_all_observers() -> None:
    a, b = StubRunObserver(), StubRunObserver()
    comp = CompositeRunObserver(a, b)
    comp.on_step_end("s", RunStatus.SUCCESS, 0, 0, 10)
    assert a.step_statuses == ["success"]
    assert b.step_statuses == ["success"]


def test_composite_fans_out_pipeline_events() -> None:
    a, b = StubRunObserver(), StubRunObserver()
    comp = CompositeRunObserver(a, b)
    comp.on_pipeline_start(object(), object(), "run-1")
    comp.on_pipeline_end("run-1", RunStatus.SUCCESS, 200)
    assert a.pipeline_statuses == ["success"]
    assert b.pipeline_statuses == ["success"]


def test_composite_fans_out_step_error() -> None:
    a, b = StubRunObserver(), StubRunObserver()
    comp = CompositeRunObserver(a, b)
    comp.on_step_error("s", RuntimeError("fail"))
    assert "step_error" in a.event_names
    assert "step_error" in b.event_names


def test_composite_three_observers() -> None:
    observers = [StubRunObserver() for _ in range(3)]
    comp = CompositeRunObserver(*observers)
    comp.on_step_end("s", RunStatus.FAILED, 0, 0, 1)
    for obs in observers:
        assert obs.step_statuses == ["failed"]
