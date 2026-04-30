"""Protocol and composite observer tests for streaming observability."""

from __future__ import annotations

from loom.streaming.observability import (
    CompositeFlowObserver,
    NoopFlowObserver,
    StreamingFlowObserver,
    StructlogFlowObserver,
)
from tests.unit.streaming.observability.cases import FailingFlowObserver, RecordingFlowObserver


def test_noop_flow_observer_matches_protocol() -> None:
    observer: StreamingFlowObserver = NoopFlowObserver()

    observer.on_flow_start("test", node_count=3)
    observer.on_flow_end("test", status="success", duration_ms=100)
    observer.on_node_start("test", 0, node_type="Step")
    observer.on_node_end("test", 0, node_type="Step", status="success", duration_ms=10)
    observer.on_node_error("test", 0, node_type="Step", exc=RuntimeError("err"))
    observer.on_collect_batch(
        "test",
        0,
        node_type="CollectBatch",
        batch_size=2,
        max_records=4,
        timeout_ms=1000,
        reason="timeout_or_flush",
    )


def test_structlog_flow_observer_matches_protocol() -> None:
    observer: StreamingFlowObserver = StructlogFlowObserver()
    assert isinstance(observer, StreamingFlowObserver)


def test_composite_flow_observer_fans_out_events() -> None:
    first = RecordingFlowObserver()
    second = RecordingFlowObserver()
    composite = CompositeFlowObserver([first, second])

    composite.on_flow_start("my_flow", node_count=2)
    composite.on_node_start("my_flow", 0, node_type="Step")
    composite.on_node_end("my_flow", 0, node_type="Step", status="success", duration_ms=5)
    composite.on_collect_batch(
        "my_flow",
        1,
        node_type="CollectBatch",
        batch_size=1,
        max_records=2,
        timeout_ms=100,
        reason="size",
    )
    composite.on_flow_end("my_flow", status="success", duration_ms=100)

    assert first.events == second.events
    assert len(first.events) == 5
    assert first.events[0] == ("flow_start", "my_flow", "2")
    assert first.events[1] == ("node_start", "my_flow", "0", "Step")


def test_composite_flow_observer_isolates_errors() -> None:
    good = RecordingFlowObserver()
    composite = CompositeFlowObserver([FailingFlowObserver(), good])

    composite.on_flow_start("test", node_count=1)
    composite.on_node_start("test", 0, node_type="Step")
    composite.on_node_error("test", 0, node_type="Step", exc=ValueError("bad"))
    composite.on_flow_end("test", status="failed", duration_ms=50)

    assert len(good.events) == 4


def test_composite_flow_observer_empty_is_noop() -> None:
    composite = CompositeFlowObserver([])
    composite.on_flow_start("test", node_count=0)
    composite.on_flow_end("test", status="success", duration_ms=0)
