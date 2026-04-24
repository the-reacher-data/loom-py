"""Tests for streaming flow observability and adapter node handler registry."""

from __future__ import annotations

from types import MappingProxyType
from unittest.mock import patch

import pytest

from loom.streaming.nodes._step import RecordStep
from loom.streaming.observability import (
    CompositeFlowObserver,
    NoopFlowObserver,
    StreamingFlowObserver,
    StructlogFlowObserver,
)
from loom.streaming.testing import StreamingTestRunner
from tests.unit.streaming.support.flow_cases import StreamFlowCase

# ---------------------------------------------------------------------------
# Recording observer for assertions
# ---------------------------------------------------------------------------


class _RecordingFlowObserver:
    """Observer that records all events for test assertions."""

    def __init__(self) -> None:
        self.events: list[tuple[str, ...]] = []

    def on_flow_start(self, flow_name: str, *, node_count: int) -> None:
        self.events.append(("flow_start", flow_name, str(node_count)))

    def on_flow_end(self, flow_name: str, *, status: str, duration_ms: int) -> None:
        self.events.append(("flow_end", flow_name, status, str(duration_ms)))

    def on_node_start(self, flow_name: str, node_idx: int, *, node_type: str) -> None:
        self.events.append(("node_start", flow_name, str(node_idx), node_type))

    def on_node_end(
        self, flow_name: str, node_idx: int, *, node_type: str, status: str, duration_ms: int
    ) -> None:
        self.events.append(("node_end", flow_name, str(node_idx), node_type, status))

    def on_node_error(
        self, flow_name: str, node_idx: int, *, node_type: str, exc: Exception
    ) -> None:
        self.events.append(("node_error", flow_name, str(node_idx), node_type, repr(exc)))


class _FailingFlowObserver:
    """Observer that always raises to test error isolation."""

    def on_flow_start(self, flow_name: str, *, node_count: int) -> None:
        raise RuntimeError("boom")

    def on_flow_end(self, flow_name: str, *, status: str, duration_ms: int) -> None:
        raise RuntimeError("boom")

    def on_node_start(self, flow_name: str, node_idx: int, *, node_type: str) -> None:
        raise RuntimeError("boom")

    def on_node_end(
        self, flow_name: str, node_idx: int, *, node_type: str, status: str, duration_ms: int
    ) -> None:
        raise RuntimeError("boom")

    def on_node_error(
        self, flow_name: str, node_idx: int, *, node_type: str, exc: Exception
    ) -> None:
        raise RuntimeError("boom")


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


def test_noop_flow_observer_matches_protocol() -> None:
    observer: StreamingFlowObserver = NoopFlowObserver()

    observer.on_flow_start("test", node_count=3)
    observer.on_flow_end("test", status="success", duration_ms=100)
    observer.on_node_start("test", 0, node_type="Step")
    observer.on_node_end("test", 0, node_type="Step", status="success", duration_ms=10)
    observer.on_node_error("test", 0, node_type="Step", exc=RuntimeError("err"))


def test_structlog_flow_observer_matches_protocol() -> None:
    observer: StreamingFlowObserver = StructlogFlowObserver()
    assert isinstance(observer, StreamingFlowObserver)


def test_recording_observer_matches_protocol() -> None:
    observer = _RecordingFlowObserver()
    assert isinstance(observer, StreamingFlowObserver)


# ---------------------------------------------------------------------------
# Composite fan-out and error isolation
# ---------------------------------------------------------------------------


def test_composite_flow_observer_fans_out_events() -> None:
    first = _RecordingFlowObserver()
    second = _RecordingFlowObserver()
    composite = CompositeFlowObserver([first, second])

    composite.on_flow_start("my_flow", node_count=2)
    composite.on_node_start("my_flow", 0, node_type="Step")
    composite.on_node_end("my_flow", 0, node_type="Step", status="success", duration_ms=5)
    composite.on_flow_end("my_flow", status="success", duration_ms=100)

    assert first.events == second.events
    assert len(first.events) == 4
    assert first.events[0] == ("flow_start", "my_flow", "2")
    assert first.events[1] == ("node_start", "my_flow", "0", "Step")


def test_composite_flow_observer_isolates_errors() -> None:
    good = _RecordingFlowObserver()
    composite = CompositeFlowObserver([_FailingFlowObserver(), good])

    composite.on_flow_start("test", node_count=1)
    composite.on_node_start("test", 0, node_type="Step")
    composite.on_node_error("test", 0, node_type="Step", exc=ValueError("bad"))
    composite.on_flow_end("test", status="failed", duration_ms=50)

    assert len(good.events) == 4


def test_composite_flow_observer_empty_is_noop() -> None:
    composite = CompositeFlowObserver([])
    composite.on_flow_start("test", node_count=0)
    composite.on_flow_end("test", status="success", duration_ms=0)


# ---------------------------------------------------------------------------
# Structlog observer
# ---------------------------------------------------------------------------


def test_structlog_flow_observer_logs_flow_start() -> None:
    observer = StructlogFlowObserver()

    with patch("loom.streaming.observability.observers.structlog._flow_log") as log:
        observer.on_flow_start("orders", node_count=3)

    log.info.assert_called_once_with("flow_start", flow="orders", node_count=3)


def test_structlog_flow_observer_logs_flow_end() -> None:
    observer = StructlogFlowObserver()

    with patch("loom.streaming.observability.observers.structlog._flow_log") as log:
        observer.on_flow_end("orders", status="success", duration_ms=150)

    log.info.assert_called_once_with("flow_end", flow="orders", status="success", duration_ms=150)


def test_structlog_flow_observer_logs_node_start_at_debug() -> None:
    observer = StructlogFlowObserver()

    with patch("loom.streaming.observability.observers.structlog._flow_log") as log:
        observer.on_node_start("orders", 0, node_type="FakeStep")

    log.debug.assert_called_once_with("node_start", flow="orders", node_idx=0, node_type="FakeStep")


def test_structlog_flow_observer_logs_node_end_at_debug() -> None:
    observer = StructlogFlowObserver()

    with patch("loom.streaming.observability.observers.structlog._flow_log") as log:
        observer.on_node_end("orders", 0, node_type="FakeStep", status="success", duration_ms=5)

    log.debug.assert_called_once_with(
        "node_end",
        flow="orders",
        node_idx=0,
        node_type="FakeStep",
        status="success",
        duration_ms=5,
    )


def test_structlog_flow_observer_warns_on_slow_node() -> None:
    observer = StructlogFlowObserver(slow_node_threshold_ms=100)

    with patch("loom.streaming.observability.observers.structlog._flow_log") as log:
        observer.on_node_end("orders", 0, node_type="SlowStep", status="success", duration_ms=200)

    log.warning.assert_called_once_with(
        "slow_node",
        flow="orders",
        node_idx=0,
        node_type="SlowStep",
        duration_ms=200,
        threshold_ms=100,
    )


def test_structlog_flow_observer_no_slow_warning_below_threshold() -> None:
    observer = StructlogFlowObserver(slow_node_threshold_ms=100)

    with patch("loom.streaming.observability.observers.structlog._flow_log") as log:
        observer.on_node_end("orders", 0, node_type="FastStep", status="success", duration_ms=50)

    assert log.warning.call_count == 0


def test_structlog_flow_observer_logs_node_error() -> None:
    observer = StructlogFlowObserver()

    with patch("loom.streaming.observability.observers.structlog._flow_log") as log:
        observer.on_node_error("orders", 0, node_type="FakeStep", exc=ValueError("bad input"))

    log.error.assert_called_once_with(
        "node_error",
        flow="orders",
        node_idx=0,
        node_type="FakeStep",
        error="ValueError('bad input')",
    )


# ---------------------------------------------------------------------------
# _NODE_HANDLERS immutability
# ---------------------------------------------------------------------------


def test_node_handlers_is_immutable() -> None:
    from loom.streaming.bytewax._adapter import _NODE_HANDLERS

    assert isinstance(_NODE_HANDLERS, MappingProxyType)

    with pytest.raises(TypeError):
        _NODE_HANDLERS[object] = lambda *a: None  # type: ignore[index]


def test_node_handlers_cover_step_handlers() -> None:
    from loom.streaming.bytewax._adapter import _NODE_HANDLERS

    assert RecordStep in _NODE_HANDLERS


def test_streaming_test_runner_emits_flow_observer_events_for_async_flow(
    async_flow_case: StreamFlowCase,
) -> None:
    observer = _RecordingFlowObserver()
    runner = StreamingTestRunner.from_flow(
        async_flow_case.flow,
        runtime_config=async_flow_case.config,
        observer=observer,
    ).with_messages(list(async_flow_case.input_messages))

    runner.run()

    assert observer.events[0] == (
        "flow_start",
        async_flow_case.flow.name,
        str(len(async_flow_case.flow.process.nodes)),
    )
    assert observer.events[-1][0] == "flow_end"
    assert any(event[0] == "node_start" and event[-1] == "WithAsync" for event in observer.events)
    assert any(event[0] == "node_end" and event[3] == "WithAsync" for event in observer.events)
