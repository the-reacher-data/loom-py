"""Runner integration tests for streaming observability."""

from __future__ import annotations

from loom.streaming import Message, MessageMeta, StreamFlow
from loom.streaming.testing import StreamingTestRunner
from tests.unit.streaming.flows.cases import StreamFlowCase
from tests.unit.streaming.observability.cases import DropItem, RecordingFlowObserver


def test_unrouted_error_notifies_observer(
    drop_flow: StreamFlow[DropItem, DropItem],
    drop_item: DropItem,
    recording_flow_observer: RecordingFlowObserver,
) -> None:
    """Observer receives node_error for unrouted task errors."""
    config = {
        "kafka": {
            "consumer": {"brokers": ["localhost:9092"], "group_id": "g", "topics": ["items"]},
            "producer": {"brokers": ["localhost:9092"], "topic": "items.out"},
        }
    }
    runner = StreamingTestRunner.from_dict(
        drop_flow,
        config=config,
        observer=recording_flow_observer,
    )
    msg = Message(payload=drop_item, meta=MessageMeta(message_id="m1"))

    runner.with_messages([msg]).run()

    node_errors = [event for event in recording_flow_observer.events if event[0] == "node_error"]
    assert any("unrouted_task_error" in event[3] for event in node_errors)


def test_streaming_test_runner_emits_flow_observer_events_for_async_flow(
    async_flow_case: StreamFlowCase,
    recording_flow_observer: RecordingFlowObserver,
) -> None:
    runner = StreamingTestRunner.from_flow(
        async_flow_case.flow,
        runtime_config=async_flow_case.config,
        observer=recording_flow_observer,
    ).with_messages(list(async_flow_case.input_messages))

    runner.run()

    assert recording_flow_observer.events[0] == (
        "flow_start",
        async_flow_case.flow.name,
        str(len(async_flow_case.flow.process.nodes)),
    )
    assert recording_flow_observer.events[-1][0] == "flow_end"
    assert any(
        event[0] == "node_start" and event[-1] == "WithAsync"
        for event in recording_flow_observer.events
    )
    assert any(
        event[0] == "node_end" and event[3] == "WithAsync"
        for event in recording_flow_observer.events
    )
