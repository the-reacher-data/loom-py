"""Structlog observer tests for streaming observability."""

from __future__ import annotations

from unittest.mock import patch

from loom.streaming.observability import StructlogFlowObserver


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
