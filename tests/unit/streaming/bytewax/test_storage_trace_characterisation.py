"""Characterisation of the storage sink's batch trace attribution — CURRENT behaviour.

``_StorageSinkPartition.write_batch`` labels the whole epoch flush with
``items[0].meta.trace_id``. One arbitrary message donates its trace to N
messages: that one message gets a WRITE span it did not cause alone, and the
other N-1 get no write span at all.

The assertions below pin that defect on purpose. A later PR changes the
attribution; this file is where that change has to show up.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from loom.core.observability.event import EventKind, LifecycleEvent, Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.streaming.bytewax.handlers import storage as _storage
from loom.streaming.core._message import Message, MessageMeta
from tests.unit.streaming.compiler.cases import Result

pytestmark = pytest.mark.bytewax

_TRACE_IDS = (
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1",
    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb2",
    "ccccccccccccccccccccccccccccccc3",
)


class _RecordingObserver:
    def __init__(self) -> None:
        self.events: list[LifecycleEvent] = []

    def on_event(self, event: LifecycleEvent) -> None:
        self.events.append(event)


class _RecordingPartition:
    def __init__(self) -> None:
        self.batches: list[Sequence[Any]] = []

    def write_batch(self, items: Sequence[Any]) -> None:
        self.batches.append(list(items))

    def close(self) -> None:
        return None


def _message(index: int) -> Message[Result]:
    return Message(
        payload=Result(value=f"row-{index}"),
        meta=MessageMeta(message_id=f"msg-{index}", trace_id=_TRACE_IDS[index]),
    )


class TestStorageBatchTraceAttribution:
    def test_write_span_takes_the_first_message_trace_and_drops_the_rest(self) -> None:
        recorder = _RecordingObserver()
        partition = _RecordingPartition()
        sink = _storage._StorageSinkPartition(
            partition,
            node_name="results_sink",
            flow_name="orders",
            observer=ObservabilityRuntime([recorder]),
        )

        sink.write_batch([_message(0), _message(1), _message(2)])

        expected_payloads = [Result(value=f"row-{i}") for i in range(3)]
        assert partition.batches == [expected_payloads]

        write_events = [event for event in recorder.events if event.scope is Scope.WRITE]
        assert [event.kind for event in write_events] == [EventKind.START, EventKind.END]

        # BROKEN TODAY: three messages, three traces, one WRITE span — and it is
        # attributed to items[0]. Messages 1 and 2 have no write span anywhere,
        # so their story ends at the node that produced them. A later PR changes
        # this attribution and must invert these assertions.
        emitted_trace_ids = {event.trace_id for event in recorder.events}
        assert emitted_trace_ids == {_TRACE_IDS[0]}
        assert _TRACE_IDS[1] not in emitted_trace_ids
        assert _TRACE_IDS[2] not in emitted_trace_ids
        assert write_events[0].meta["batch_size"] == 3

    def test_missing_first_trace_mints_a_fresh_id_unrelated_to_every_message(self) -> None:
        def _write_once() -> LifecycleEvent:
            recorder = _RecordingObserver()
            untraced = Message(
                payload=Result(value="row-0"),
                meta=MessageMeta(message_id="msg-0", trace_id=None),
            )
            sink = _storage._StorageSinkPartition(
                _RecordingPartition(),
                node_name="results_sink",
                flow_name="orders",
                observer=ObservabilityRuntime([recorder]),
            )
            sink.write_batch([untraced, _message(1)])

            write_events = [event for event in recorder.events if event.scope is Scope.WRITE]
            assert [event.kind for event in write_events] == [EventKind.START, EventKind.END]
            return write_events[0]

        first = _write_once()
        second = _write_once()

        # BROKEN TODAY: when items[0] carries no trace, ``uuid4().hex`` is minted
        # per call, so the single WRITE span belongs to no message in the batch —
        # not even the one that donated its position, and message 1's own trace
        # is nowhere in the emitted events. The id is random rather than derived
        # from the batch, so two identical batches get two unrelated stories.
        for event in (first, second):
            assert event.trace_id is not None
            assert len(event.trace_id) == 32
            int(event.trace_id, 16)
            assert event.trace_id not in _TRACE_IDS
            assert event.trace_id != "msg-0"
        assert first.trace_id != second.trace_id
