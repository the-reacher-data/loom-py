"""Commit accounting across nodes that change the record count.

Every node that turns one input record into N outputs shares one hazard: all N
outputs carry the same source offset, and each one completes that offset when it
reaches a terminal. The tracker expects exactly one completion per record, so a
node that fans out must raise that expectation before any output can be
completed, and a node that produces nothing must release the record itself.

Both failures are silent, which is why they are asserted here against a real
``KafkaCommitTracker`` and its committed offsets rather than against a spy:

- fan-out without a fork commits the offset after the *first* output, losing the
  remaining N-1 on a crash — the consumer group has already moved past them;
- zero outputs without a completion freezes the partition's watermark forever.
"""

from __future__ import annotations

from typing import Any

import pytest
from confluent_kafka import TopicPartition

from loom.streaming.bytewax import RuntimeConfigurationError, _adapter
from loom.streaming.bytewax._commit_tracker import KafkaCommitTracker
from loom.streaming.bytewax.handlers import routing as _routing
from loom.streaming.bytewax.handlers import steps as _steps
from loom.streaming.core._message import Message, MessageMeta
from loom.streaming.kafka._record import KafkaRecord
from tests.unit.streaming.bytewax.cases import Order, Result

pytestmark = pytest.mark.bytewax

TOPIC = "orders.in"
PARTITION = 0
OFFSET = 7


class _Committer:
    """Minimal committer capturing what the tracker sent to the broker."""

    def __init__(self) -> None:
        self.commits: list[list[TopicPartition]] = []

    def commit_offset(
        self, partitions: list[TopicPartition], *, asynchronous: bool = False
    ) -> None:
        del asynchronous
        self.commits.append(list(partitions))


def _sourced_message(payload: Order, offset: int = OFFSET) -> Message[Order]:
    """Build a message carrying real Kafka coordinates, as the source emits."""
    return Message(
        payload=payload,
        meta=MessageMeta(message_id="m-1", topic=TOPIC, partition=PARTITION, offset=offset),
    )


def _tracker(*offsets: int) -> tuple[KafkaCommitTracker, _Committer]:
    tracker = KafkaCommitTracker()
    committer = _Committer()
    tracker.attach_partition(TOPIC, PARTITION, committer, None)
    for offset in offsets:
        tracker.register_record(
            KafkaRecord(topic=TOPIC, key=None, value=b"raw", partition=PARTITION, offset=offset)
        )
    return tracker, committer


def _complete(tracker: KafkaCommitTracker, results: list[Any]) -> None:
    """Complete every produced output, as its terminal sink would."""
    for item in results:
        tracker.complete(item.meta.topic, item.meta.partition, item.meta.offset)


def _ctx(tracker: KafkaCommitTracker | None) -> Any:
    from types import SimpleNamespace

    from loom.core.observability.runtime import ObservabilityRuntime

    return SimpleNamespace(
        plan=SimpleNamespace(name="orders"),
        current_path=(),
        flow_runtime=ObservabilityRuntime.noop(),
        commit_tracker=tracker,
    )


def _drive(monkeypatch: pytest.MonkeyPatch, module: Any, inputs: Any) -> list[Any]:
    """Run one handler's map step directly and return what it produced."""
    produced: list[Any] = []

    def _bw_map(step_id: str, stream: object, fn: Any) -> object:
        del step_id, stream
        result = fn(inputs)
        produced.append(result)
        return result

    monkeypatch.setattr(module, "bw_map", _bw_map)
    monkeypatch.setattr("bytewax.operators.flat_map", lambda step_id, stream, fn: fn(stream))
    monkeypatch.setattr(module, "_split_node_result", lambda stream, *_: stream, raising=False)
    return produced


class _ThreeWayExpand:
    """Expand step turning one order into three results."""

    def execute(self, message: Message[Order], **_: object) -> list[Result]:
        return [Result(value=f"{message.payload.order_id}-{index}") for index in range(3)]


class _EmptyExpand:
    """Expand step that legitimately produces nothing for this record."""

    def execute(self, message: Message[Order], **_: object) -> list[Result]:
        del message
        return []


class TestExpandingStepAccounting:
    """One record in, N records out."""

    def test_offset_is_not_committed_until_every_output_completes(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        tracker, committer = _tracker(OFFSET)
        produced = _drive(monkeypatch, _steps, _sourced_message(Order(order_id="ab")))
        _steps._apply_expand_step("in", _ThreeWayExpand(), 1, _ctx(tracker))
        results = produced[0]
        assert len(results) == 3

        tracker.complete(TOPIC, PARTITION, OFFSET)
        assert tracker.flush_partition(TOPIC, PARTITION) == [], (
            "the offset was released after the first of three outputs; a crash here "
            "loses the other two permanently"
        )

        _complete(tracker, results[1:])
        assert tracker.flush_partition(TOPIC, PARTITION) == [
            TopicPartition(TOPIC, PARTITION, OFFSET + 1)
        ]
        assert len(committer.commits) == 1

    def test_a_record_producing_nothing_is_released(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        tracker, _ = _tracker(OFFSET)
        produced = _drive(monkeypatch, _steps, _sourced_message(Order(order_id="ab")))
        _steps._apply_expand_step("in", _EmptyExpand(), 1, _ctx(tracker))

        assert produced[0] == []
        assert tracker.flush_partition(TOPIC, PARTITION) == [
            TopicPartition(TOPIC, PARTITION, OFFSET + 1)
        ], "a record that produced no output was never completed: the watermark froze"

    def test_accounting_is_inert_under_at_most_once(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """With no tracker the node must behave exactly as before."""
        produced = _drive(monkeypatch, _steps, _sourced_message(Order(order_id="ab")))
        _steps._apply_expand_step("in", _ThreeWayExpand(), 1, _ctx(None))

        assert len(produced[0]) == 3


class TestExpandRoutesAccounting:
    """One record in, one message per produced ROW out — not per declared route.

    The two counts are unrelated: a flow may declare three routes and receive a
    record that yields a single row, or declare one route that yields twenty.
    Accounting by the declared count froze the partition in the first case and
    released the offset early in the second.
    """

    @staticmethod
    def _expanded(rows_by_type: dict[type, list[Any]]) -> Message[Any]:
        return Message(
            payload=rows_by_type,
            meta=MessageMeta(message_id="m-1", topic=TOPIC, partition=PARTITION, offset=OFFSET),
        )

    def test_fewer_rows_than_routes_still_commits(self) -> None:
        tracker, _ = _tracker(OFFSET)
        declared = frozenset({Order, Result})

        _routing._register_row_fanout(
            self._expanded({Order: [Order(order_id="a")]}), tracker, declared, False
        )
        tracker.complete(TOPIC, PARTITION, OFFSET)

        assert tracker.flush_partition(TOPIC, PARTITION) == [
            TopicPartition(TOPIC, PARTITION, OFFSET + 1)
        ], "two routes were declared but one row was produced: the watermark froze"

    def test_more_rows_than_routes_waits_for_every_row(self) -> None:
        tracker, _ = _tracker(OFFSET)
        declared = frozenset({Order})
        rows = [Order(order_id=str(index)) for index in range(4)]

        _routing._register_row_fanout(self._expanded({Order: rows}), tracker, declared, False)
        tracker.complete(TOPIC, PARTITION, OFFSET)

        assert tracker.flush_partition(TOPIC, PARTITION) == [], (
            "one route produced four rows and the offset was released after the first"
        )
        for _ in range(3):
            tracker.complete(TOPIC, PARTITION, OFFSET)
        assert tracker.flush_partition(TOPIC, PARTITION) == [
            TopicPartition(TOPIC, PARTITION, OFFSET + 1)
        ]

    def test_no_rows_releases_the_record(self) -> None:
        tracker, _ = _tracker(OFFSET)

        _routing._register_row_fanout(self._expanded({}), tracker, frozenset({Order}), False)

        assert tracker.flush_partition(TOPIC, PARTITION) == [
            TopicPartition(TOPIC, PARTITION, OFFSET + 1)
        ], "the expander produced nothing and nobody ever completed the record"

    def test_default_route_rows_are_counted(self) -> None:
        """Undeclared types reach the default route and must be accounted for."""
        tracker, _ = _tracker(OFFSET)

        _routing._register_row_fanout(
            self._expanded({Order: [Order(order_id="a")], Result: [Result(value="x")]}),
            tracker,
            frozenset({Order}),
            True,
        )
        tracker.complete(TOPIC, PARTITION, OFFSET)

        assert tracker.flush_partition(TOPIC, PARTITION) == [], (
            "the default route's row was not counted, so the offset was released early"
        )
        tracker.complete(TOPIC, PARTITION, OFFSET)
        assert tracker.flush_partition(TOPIC, PARTITION) == [
            TopicPartition(TOPIC, PARTITION, OFFSET + 1)
        ]

    def test_undeclared_rows_are_ignored_without_a_default_route(self) -> None:
        """With no default route those rows reach nothing and must not be expected."""
        tracker, _ = _tracker(OFFSET)

        _routing._register_row_fanout(
            self._expanded({Order: [Order(order_id="a")], Result: [Result(value="x")]}),
            tracker,
            frozenset({Order}),
            False,
        )
        tracker.complete(TOPIC, PARTITION, OFFSET)

        assert tracker.flush_partition(TOPIC, PARTITION) == [
            TopicPartition(TOPIC, PARTITION, OFFSET + 1)
        ], "a row with no route was counted as a pending completion: the watermark froze"


class TestSinkTrackingContract:
    """A sink that cannot receive the tracker must stop the flow, not the commits."""

    def test_untrackable_sink_is_rejected_at_assembly(self) -> None:
        class _UserSink:
            """A sink a user wrote without knowing about commit tracking."""

        tracker, _ = _tracker()

        with pytest.raises(RuntimeConfigurationError, match="bind_commit_tracker"):
            _adapter._bind_commit_tracker_object(_UserSink(), tracker)

    def test_untrackable_sink_is_fine_under_at_most_once(self) -> None:
        class _UserSink:
            """No tracker means nothing to bind and nothing to guarantee."""

        _adapter._bind_commit_tracker_object(_UserSink(), None)
