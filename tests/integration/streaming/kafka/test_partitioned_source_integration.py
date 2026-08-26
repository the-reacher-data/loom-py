"""End-to-end validation of ``KafkaPartitionedSource`` against a real broker.

These are *narrow integration tests*: the Bytewax source and the commit tracker
are exercised against a live Kafka-protocol broker, because the properties under
test are broker behaviours that no fake can establish.

The design premise being validated is that the partitioned source pins each
partition with ``assign`` and **never** calls ``subscribe``, using the consumer
group purely as an offset store. Everything downstream — resume, the commit
floor, the retention keep-alive — rests on ``OffsetFetch``/``OffsetCommit``
working without group membership, so that assumption is asserted directly
rather than inferred.

Every assertion about a committed offset is read back **from the broker** with
an independent consumer, never from the tracker's own in-memory state.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence

import pytest

from loom.streaming.bytewax._commit_tracker import KafkaCommitTracker
from loom.streaming.bytewax._runtime_io import KafkaPartitionedSource
from loom.streaming.compiler._plan import CompiledSingleSource
from loom.streaming.kafka._errors import KafkaPollError
from loom.streaming.kafka._record import KafkaRecord
from loom.streaming.kafka.client._consumer import KafkaConsumerClient

pytestmark = [pytest.mark.integration, pytest.mark.kafka]

_DRAIN_TIMEOUT_S = 30.0
_DRAIN_IDLE_POLL_S = 0.05
# Bounded window for asserting that nothing arrives. Proving a negative needs a
# deadline, not the full drain timeout — that would add 30s of pure waiting.
_QUIET_WINDOW_S = 2.0
_COMMITTED_TIMEOUT_MS = 10_000

SourceFactory = Callable[..., CompiledSingleSource]
TopicFactory = Callable[[int], str]
Producer = Callable[[str, Sequence[tuple[int, bytes]]], None]


def _drain(partition: object, expected: int) -> list[KafkaRecord[bytes]]:
    """Poll one source partition until ``expected`` records arrive or time out.

    ``next_batch`` returns whatever librdkafka already buffered, so a batch may
    legitimately be empty right after ``assign``. Waiting on a count with a
    deadline is what makes the test deterministic without being flaky.
    """
    collected: list[KafkaRecord[bytes]] = []
    deadline = time.monotonic() + _DRAIN_TIMEOUT_S
    while len(collected) < expected and time.monotonic() < deadline:
        batch = partition.next_batch()  # type: ignore[attr-defined]
        if not batch:
            time.sleep(_DRAIN_IDLE_POLL_S)
            continue
        collected.extend(batch)
    return collected


def _drain_quiet(partition: object) -> list[KafkaRecord[bytes]]:
    """Poll a partition for a bounded window and return everything it yielded."""
    collected: list[KafkaRecord[bytes]] = []
    deadline = time.monotonic() + _QUIET_WINDOW_S
    while time.monotonic() < deadline:
        collected.extend(partition.next_batch())  # type: ignore[attr-defined]
        time.sleep(_DRAIN_IDLE_POLL_S)
    return collected


def _complete_all(
    tracker: KafkaCommitTracker,
    records: Sequence[KafkaRecord[bytes]],
) -> None:
    """Mark every drained record as fully processed downstream."""
    for record in records:
        assert record.partition is not None
        assert record.offset is not None
        tracker.complete(record.topic, record.partition, record.offset)


def _committed(bootstrap: str, group: str, topic: str, partition: int) -> int | None:
    """Read the group's committed offset with an independent consumer."""
    from loom.streaming.kafka._config import ConsumerSettings

    settings = ConsumerSettings(
        brokers=(bootstrap,),
        group_id=group,
        topics=(topic,),
        delivery="at_least_once",
    )
    client = KafkaConsumerClient.unassigned(settings)
    try:
        return client.committed_offset(topic, partition, timeout_ms=_COMMITTED_TIMEOUT_MS)
    finally:
        client.close()


def _payloads(count: int, partition: int) -> list[tuple[int, bytes]]:
    return [(partition, f"p{partition}-r{index}".encode()) for index in range(count)]


class TestPartitionDiscovery:
    """``list_parts`` against real broker metadata."""

    def test_lists_one_key_per_kafka_partition(
        self,
        topic_factory: TopicFactory,
        make_source: SourceFactory,
        group_id: str,
    ) -> None:
        topic = topic_factory(3)
        source = KafkaPartitionedSource(make_source(topic, group_id))

        assert source.list_parts() == [f"{topic}:0", f"{topic}:1", f"{topic}:2"]

    def test_missing_topic_fails_loudly(
        self,
        make_source: SourceFactory,
        group_id: str,
    ) -> None:
        """A missing topic must raise, never silently become a 1-partition topic."""
        source = KafkaPartitionedSource(make_source("loom.it.topic.does.not.exist", group_id))

        with pytest.raises(KafkaPollError, match="cannot list partitions"):
            source.list_parts()


class TestGroupAsOffsetStore:
    """The central design premise: offsets without group membership."""

    def test_committed_offset_answers_without_membership(
        self,
        bootstrap: str,
        topic_factory: TopicFactory,
        make_source: SourceFactory,
        group_id: str,
    ) -> None:
        """An unassigned, never-subscribed consumer can query the coordinator."""
        topic = topic_factory(1)
        del make_source

        assert _committed(bootstrap, group_id, topic, 0) is None

    def test_assign_mode_commit_persists_in_the_group(
        self,
        bootstrap: str,
        topic_factory: TopicFactory,
        make_source: SourceFactory,
        produce: Producer,
        group_id: str,
    ) -> None:
        """Offsets committed by an assign-mode consumer are readable by another."""
        topic = topic_factory(1)
        produce(topic, _payloads(5, partition=0))
        tracker = KafkaCommitTracker()
        source = KafkaPartitionedSource(make_source(topic, group_id), tracker)

        part = source.build_part("step", f"{topic}:0", None)
        records = _drain(part, 5)
        _complete_all(tracker, records)
        part.close()

        assert len(records) == 5
        assert _committed(bootstrap, group_id, topic, 0) == 5


class TestDeliveryIntegrity:
    """No loss, no duplication, broker order preserved per partition."""

    def test_every_record_is_read_exactly_once_across_partitions(
        self,
        topic_factory: TopicFactory,
        make_source: SourceFactory,
        produce: Producer,
        group_id: str,
    ) -> None:
        topic = topic_factory(3)
        per_partition = 20
        for partition in range(3):
            produce(topic, _payloads(per_partition, partition))
        tracker = KafkaCommitTracker()
        source = KafkaPartitionedSource(make_source(topic, group_id), tracker)

        seen: dict[int, list[bytes]] = {}
        for partition in range(3):
            part = source.build_part("step", f"{topic}:{partition}", None)
            records = _drain(part, per_partition)
            _complete_all(tracker, records)
            part.close()
            seen[partition] = [record.value for record in records]

        for partition in range(3):
            expected = [value for _, value in _payloads(per_partition, partition)]
            assert seen[partition] == expected, f"partition {partition} lost or reordered records"


class TestResume:
    """Restart semantics — the property that makes the source substitutable."""

    def test_rebuilt_partition_resumes_at_the_committed_offset(
        self,
        topic_factory: TopicFactory,
        make_source: SourceFactory,
        produce: Producer,
        group_id: str,
    ) -> None:
        """A fresh run with no recovery state reads only what came after the commit."""
        topic = topic_factory(1)
        produce(topic, _payloads(4, partition=0))
        first_tracker = KafkaCommitTracker()
        first_source = KafkaPartitionedSource(make_source(topic, group_id), first_tracker)

        first_part = first_source.build_part("step", f"{topic}:0", None)
        first_records = _drain(first_part, 4)
        _complete_all(first_tracker, first_records)
        first_part.close()

        produce(topic, [(0, b"after-restart-1"), (0, b"after-restart-2")])
        second_tracker = KafkaCommitTracker()
        second_source = KafkaPartitionedSource(make_source(topic, group_id), second_tracker)
        second_part = second_source.build_part("step", f"{topic}:0", None)
        second_records = _drain(second_part, 2)
        second_part.close()

        assert [record.value for record in second_records] == [
            b"after-restart-1",
            b"after-restart-2",
        ], "resume replayed already-committed records or skipped new ones"


class TestCommitFloor:
    """A recovery replay must never rewind the consumer-group watermark."""

    def test_commits_below_the_floor_are_suppressed(
        self,
        bootstrap: str,
        topic_factory: TopicFactory,
        make_source: SourceFactory,
        produce: Producer,
        group_id: str,
    ) -> None:
        topic = topic_factory(1)
        produce(topic, _payloads(6, partition=0))
        first_tracker = KafkaCommitTracker()
        first_source = KafkaPartitionedSource(make_source(topic, group_id), first_tracker)
        first_part = first_source.build_part("step", f"{topic}:0", None)
        _complete_all(first_tracker, _drain(first_part, 6))
        first_part.close()
        assert _committed(bootstrap, group_id, topic, 0) == 6

        # Recovery snapshot deliberately behind the group offset: replay offsets
        # 2..5, complete them, and the group watermark must not move backwards.
        replay_tracker = KafkaCommitTracker()
        replay_source = KafkaPartitionedSource(make_source(topic, group_id), replay_tracker)
        replay_part = replay_source.build_part("step", f"{topic}:0", 2)
        _complete_all(replay_tracker, _drain(replay_part, 4))
        replay_part.close()

        assert _committed(bootstrap, group_id, topic, 0) == 6, (
            "a replay from a stale recovery snapshot rewound the group offset"
        )


class TestRetentionKeepAlive:
    """Idle partitions re-commit their watermark to refresh offset retention."""

    def test_idle_partition_recommits_without_advancing(
        self,
        bootstrap: str,
        topic_factory: TopicFactory,
        make_source: SourceFactory,
        produce: Producer,
        group_id: str,
    ) -> None:
        topic = topic_factory(1)
        produce(topic, _payloads(3, partition=0))
        tracker = KafkaCommitTracker()
        source = KafkaPartitionedSource(
            make_source(topic, group_id, commit_keepalive_ms=1), tracker
        )

        part = source.build_part("step", f"{topic}:0", None)
        _complete_all(tracker, _drain(part, 3))
        part.close()
        assert _committed(bootstrap, group_id, topic, 0) == 3

        # Second run: no new traffic. The keep-alive re-commits the seeded
        # watermark on a member-less group without error and without moving it.
        idle_tracker = KafkaCommitTracker()
        idle_source = KafkaPartitionedSource(
            make_source(topic, group_id, commit_keepalive_ms=1), idle_tracker
        )
        idle_part = idle_source.build_part("step", f"{topic}:0", None)
        assert _drain_quiet(idle_part) == []
        idle_part.close()

        assert _committed(bootstrap, group_id, topic, 0) == 3
