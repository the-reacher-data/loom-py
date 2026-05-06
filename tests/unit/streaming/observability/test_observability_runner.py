"""Runner integration tests for streaming observability."""

from __future__ import annotations

import pytest

from loom.core.observability.event import EventKind, Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.streaming import Message, MessageMeta, StreamFlow
from loom.streaming.core._errors import ErrorKind
from loom.streaming.testing import StreamingTestRunner
from tests.unit.streaming.flows.cases import StreamFlowCase
from tests.unit.streaming.observability.cases import DropItem, RecordingFlowObserver

pytestmark = pytest.mark.integration


def test_unrouted_error_notifies_observer(
    drop_flow: StreamFlow[DropItem, DropItem],
    drop_item: DropItem,
    recording_flow_observer: RecordingFlowObserver,
) -> None:
    """Unrouted task errors are logged rather than fanned out."""
    config = {
        "kafka": {
            "consumer": {"brokers": ["localhost:9092"], "group_id": "g", "topics": ["items"]},
            "producer": {"brokers": ["localhost:9092"], "topic": "items.out"},
        }
    }
    runner = StreamingTestRunner.from_dict(
        drop_flow,
        config,
        observability_runtime=ObservabilityRuntime([recording_flow_observer]),
    )
    msg = Message(payload=drop_item, meta=MessageMeta(message_id="m1"))

    runner.with_messages([msg]).run()

    assert recording_flow_observer.events
    assert any(event.kind is EventKind.ERROR for event in recording_flow_observer.events)
    assert any(event.scope is Scope.NODE for event in recording_flow_observer.events)


def test_streaming_test_runner_emits_flow_observer_events_for_async_flow(
    async_flow_case: StreamFlowCase,
    recording_flow_observer: RecordingFlowObserver,
) -> None:
    runner = StreamingTestRunner.from_flow(
        async_flow_case.flow,
        runtime_config=async_flow_case.config,
        observability_runtime=ObservabilityRuntime([recording_flow_observer]),
    ).with_messages(list(async_flow_case.input_messages))

    runner.run()

    assert recording_flow_observer.events[0].scope is Scope.POLL_CYCLE
    assert recording_flow_observer.events[0].kind is EventKind.START
    assert recording_flow_observer.events[-1].scope is Scope.POLL_CYCLE
    assert recording_flow_observer.events[-1].kind is EventKind.END
    assert any(
        event.scope is Scope.NODE
        and event.kind is EventKind.START
        and event.meta.get("node_type") == "WithAsync"
        for event in recording_flow_observer.events
    )
    assert any(
        event.scope is Scope.NODE
        and event.kind is EventKind.END
        and event.meta.get("node_type") == "WithAsync"
        for event in recording_flow_observer.events
    )


def test_streaming_test_runner_reset_clears_buffers(
    drop_flow: StreamFlow[DropItem, DropItem],
) -> None:
    config = {
        "kafka": {
            "consumer": {"brokers": ["localhost:9092"], "group_id": "g", "topics": ["items"]},
            "producer": {"brokers": ["localhost:9092"], "topic": "items.out"},
        }
    }
    runner = StreamingTestRunner.from_dict(drop_flow, config)
    runner.with_messages([Message(payload=DropItem(value="x"), meta=MessageMeta(message_id="m1"))])
    runner.capture_errors(ErrorKind.TASK)
    runner.output.append(DropItem(value="y"))
    runner.errors[ErrorKind.TASK].append(DropItem(value="z"))

    runner.reset()

    assert runner.output == []
    assert runner.errors == {}
