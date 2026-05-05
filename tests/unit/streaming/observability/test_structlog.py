"""Structlog observer tests for streaming observability."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from loom.streaming.observability import StructlogFlowObserver


@pytest.mark.parametrize(
    ("method_name", "args", "kwargs", "expected_logger_method", "expected_event"),
    [
        (
            "on_flow_start",
            ("orders",),
            {"node_count": 3},
            "info",
            ("flow_start", {"flow": "orders", "node_count": 3}),
        ),
        (
            "on_flow_end",
            ("orders",),
            {"status": "success", "duration_ms": 150},
            "info",
            ("flow_end", {"flow": "orders", "status": "success", "duration_ms": 150}),
        ),
        (
            "on_node_start",
            ("orders", 0),
            {"node_type": "FakeStep"},
            "debug",
            ("node_start", {"flow": "orders", "node_idx": 0, "node_type": "FakeStep"}),
        ),
        (
            "on_node_end",
            ("orders", 0),
            {"node_type": "FakeStep", "status": "success", "duration_ms": 5},
            "debug",
            (
                "node_end",
                {
                    "flow": "orders",
                    "node_idx": 0,
                    "node_type": "FakeStep",
                    "status": "success",
                    "duration_ms": 5,
                },
            ),
        ),
        (
            "on_node_error",
            ("orders", 0),
            {"node_type": "FakeStep", "exc": ValueError("bad input")},
            "error",
            (
                "node_error",
                {
                    "flow": "orders",
                    "node_idx": 0,
                    "node_type": "FakeStep",
                    "error": "ValueError('bad input')",
                },
            ),
        ),
        (
            "on_collect_batch",
            ("orders", 2),
            {
                "node_type": "CollectBatch",
                "batch_size": 2,
                "max_records": 4,
                "timeout_ms": 500,
                "reason": "timeout_or_flush",
            },
            "info",
            (
                "collect_batch",
                {
                    "flow": "orders",
                    "node_idx": 2,
                    "node_type": "CollectBatch",
                    "batch_size": 2,
                    "max_records": 4,
                    "timeout_ms": 500,
                    "reason": "timeout_or_flush",
                },
            ),
        ),
    ],
)
def test_structlog_flow_observer_emits_expected_calls(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
    args: tuple[object, ...],
    kwargs: dict[str, object],
    expected_logger_method: str,
    expected_event: tuple[str, dict[str, object]],
) -> None:
    observer = StructlogFlowObserver()
    log = Mock()
    monkeypatch.setattr("loom.streaming.observability.observers.structlog._flow_log", log)

    getattr(observer, method_name)(*args, **kwargs)

    getattr(log, expected_logger_method).assert_called_once_with(
        expected_event[0],
        **expected_event[1],
    )


def test_structlog_flow_observer_warns_on_slow_node(monkeypatch: pytest.MonkeyPatch) -> None:
    observer = StructlogFlowObserver(slow_node_threshold_ms=100)
    log = Mock()
    monkeypatch.setattr("loom.streaming.observability.observers.structlog._flow_log", log)

    observer.on_node_end("orders", 0, node_type="SlowStep", status="success", duration_ms=200)

    log.warning.assert_called_once_with(
        "slow_node",
        flow="orders",
        node_idx=0,
        node_type="SlowStep",
        duration_ms=200,
        threshold_ms=100,
    )


def test_structlog_flow_observer_no_slow_warning_below_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observer = StructlogFlowObserver(slow_node_threshold_ms=100)
    log = Mock()
    monkeypatch.setattr("loom.streaming.observability.observers.structlog._flow_log", log)

    observer.on_node_end("orders", 0, node_type="FastStep", status="success", duration_ms=50)

    assert log.warning.call_count == 0
