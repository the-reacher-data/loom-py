"""Kafka commit tracker for the streaming Bytewax runtime.

Tracks per-offset completion across sinks, DLQs, error routes, and drops, and
commits consumer-group offsets only after every downstream branch confirmed a
record (at-least-once delivery).

Design invariants:

- **register → fork → complete**: sources register each record before emitting
  it; fan-out nodes fork; every terminal branch completes.
- **Gap-tolerant watermark**: Kafka offsets are not contiguous (transactional
  control records, compaction). The watermark waits only for offsets that were
  actually registered, so gaps never freeze commits.
- **Coalesced commits**: ``complete()`` never talks to Kafka. Watermark
  advances are accumulated per partition and committed by the explicit
  per-partition commit methods — one
  commit per partition per flush cycle instead of one round-trip per record.
- **Commit floor**: commits at or below the floor (the group offset observed
  at partition start) are suppressed, so a Bytewax-recovery replay never
  rewinds the consumer group watermark.
"""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from confluent_kafka import TopicPartition

from loom.streaming.kafka._record import KafkaRecord


@runtime_checkable
class OffsetCommitter(Protocol):
    """Minimal committer contract the tracker needs to persist offsets."""

    def commit_offset(
        self,
        partitions: list[TopicPartition],
        *,
        asynchronous: bool = False,
    ) -> None:
        """Commit explicit topic-partition offsets."""


@runtime_checkable
class CommitCompletionPort(Protocol):
    """The only tracker operations a downstream branch is allowed to reach.

    Sinks, DLQs, error routes and drop sinks run on Bytewax worker threads that
    do not own any partition. They may report what happened to a record; they
    must never drive a partition's commit lifecycle, because
    ``KafkaCommitTracker`` commits under a single-writer rule — only the thread
    owning a partition may commit it, and an out-of-order group commit from a
    sink thread would corrupt the watermark.

    Narrowing the type is what enforces that rule: with the concrete tracker in
    hand a sink could call ``close_partition`` and the type checker would agree.
    """

    def fork(self, topic: str, partition: int, offset: int, extra_outputs: int) -> None:
        """Increase the expected completions for one logical message."""

    def complete(self, topic: str, partition: int, offset: int) -> None:
        """Mark one logical message branch as complete."""


class KafkaCommitTracker:
    """Track per-offset completion and commit Kafka offsets once safe."""

    def __init__(self) -> None:
        self._partitions: dict[tuple[str, int], _PartitionCommitState] = {}
        self._messages: dict[str, _TrackedMessage] = {}
        self._lock = threading.Lock()

    def attach_partition(
        self,
        topic: str,
        partition: int,
        committer: OffsetCommitter,
        committed_offset: int | None,
    ) -> None:
        """Take ownership of one partition's commit state.

        Called when a partition is (re)built. Everything the partition needs is
        established in one step — owner, commit floor, seeded watermark, and a
        clean slate of in-flight offsets — because all of it has to happen, in
        that order, or the commit invariant breaks silently.

        The seeded watermark gives idle partitions something the retention
        keep-alive can re-commit; the floor suppresses commits at or below the
        group offset observed here, so a recovery replay can never rewind it.

        Args:
            topic: Physical topic name.
            partition: Kafka partition index.
            committer: The consumer that owns this partition's offsets.
            committed_offset: Group offset observed at partition start, or
                ``None`` when the group has none yet.
        """
        key = (topic, partition)
        with self._lock:
            self._discard_partition_messages(topic, partition)
            self._partitions[key] = _PartitionCommitState(
                watermark=_PartitionWatermark(
                    topic=topic,
                    partition=partition,
                    commit_offset=committed_offset,
                ),
                committer=committer,
                floor=committed_offset,
            )

    def _discard_partition_messages(self, topic: str, partition: int) -> None:
        """Drop in-flight offsets of one partition; caller holds the lock."""
        stale = [
            key
            for key, state in self._messages.items()
            if state.topic == topic and state.partition == partition
        ]
        for key in stale:
            self._messages.pop(key, None)

    def register_record(self, record: KafkaRecord[bytes]) -> None:
        """Register one newly polled Kafka record for offset tracking."""
        if record.partition is None or record.offset is None:
            return
        key = f"{record.topic}:{record.partition}:{record.offset}"
        with self._lock:
            state = self._partitions.get((record.topic, record.partition))
            if state is None or key in self._messages:
                return
            self._messages[key] = _TrackedMessage(
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                pending=1,
            )
            state.watermark.register(record.offset)

    def fork(self, topic: str, partition: int, offset: int, extra_outputs: int) -> None:
        """Increase the number of expected completions for one message."""
        if extra_outputs <= 0:
            return
        key = f"{topic}:{partition}:{offset}"
        with self._lock:
            state = self._messages.get(key)
            if state is None:
                return
            state.pending += extra_outputs

    def complete(self, topic: str, partition: int, offset: int) -> None:
        """Mark one logical branch as completed.

        Watermark advances are accumulated; the actual Kafka commit happens on
        the next :meth:`flush` (coalescing — never one round-trip per record).
        """
        key = f"{topic}:{partition}:{offset}"
        with self._lock:
            state = self._messages.get(key)
            if state is None:
                return
            state.pending -= 1
            if state.pending > 0:
                return
            self._messages.pop(key, None)
            partition_state = self._partitions.get((state.topic, state.partition))
            if partition_state is None:
                return
            if partition_state.watermark.complete(state.offset):
                partition_state.dirty = True

    def flush_partition(self, topic: str, partition: int) -> list[TopicPartition]:
        """Commit one partition's watermark if it advanced since the last commit.

        The hot path. The commit is handed to librdkafka asynchronously so the
        poll loop is never blocked on a broker round-trip.

        Args:
            topic: Physical topic name.
            partition: Kafka partition index.

        Returns:
            The offsets handed to the committer; empty when nothing advanced.

        Raises:
            KafkaCommitError: Propagated from the committer; the partition is
                re-marked dirty so the next flush retries it.
        """
        return self._commit_partition(topic, partition, force=False, synchronous=False)

    def keepalive_partition(self, topic: str, partition: int) -> list[TopicPartition]:
        """Re-commit one partition's current watermark to refresh retention.

        Member-less consumer groups expire committed offsets after
        ``offsets.retention.minutes``, so an idle partition re-commits the same
        offset periodically. Re-committing an unchanged offset is idempotent
        and never rewinds.

        Args:
            topic: Physical topic name.
            partition: Kafka partition index.

        Returns:
            The offsets handed to the committer.

        Raises:
            KafkaCommitError: Propagated from the committer.
        """
        return self._commit_partition(topic, partition, force=True, synchronous=False)

    def close_partition(self, topic: str, partition: int) -> list[TopicPartition]:
        """Commit one partition's final watermark, blocking until acknowledged.

        Called once while the partition shuts down. Synchronous on purpose: an
        offset lost at shutdown is reprocessed on the next run.

        Args:
            topic: Physical topic name.
            partition: Kafka partition index.

        Returns:
            The offsets handed to the committer.

        Raises:
            KafkaCommitError: Propagated from the committer.
        """
        return self._commit_partition(topic, partition, force=True, synchronous=True)

    def _commit_partition(
        self,
        topic: str,
        partition: int,
        *,
        force: bool,
        synchronous: bool,
    ) -> list[TopicPartition]:
        """Shared commit mechanics behind the three named entry points.

        Must be invoked only from the thread that owns the partition (its poll
        loop, or its close): that single-writer rule is what makes out-of-order
        group commits impossible.

        The two axes stay private precisely so callers never spell them out —
        each combination that production actually uses has its own named method.
        """
        key = (topic, partition)
        with self._lock:
            state = self._partitions.get(key)
            if state is None or not (force or state.dirty):
                return []
            offset = state.committable_offset()
            state.dirty = False
            if offset is None:
                return []
            committer = state.committer
        target = TopicPartition(topic, partition, offset)
        try:
            committer.commit_offset([target], asynchronous=not synchronous)
        except Exception:
            with self._lock:
                stale = self._partitions.get(key)
                if stale is not None:
                    stale.dirty = True
            raise
        return [target]


@dataclass(slots=True)
class _PartitionCommitState:
    """Everything the tracker owns for one Kafka topic partition.

    Held as a single record rather than parallel dictionaries keyed by
    ``(topic, partition)``: a commit decision reads the watermark, the floor and
    the owner together, and splitting them across dictionaries meant every
    decision had to re-cross them and keep four structures in step.

    Attributes:
        watermark: Gap-tolerant commit position.
        committer: Consumer that owns this partition's offsets.
        floor: Group offset observed when the partition was attached; commits
            strictly below it are suppressed.
        dirty: Whether the watermark advanced since the last commit.
    """

    watermark: _PartitionWatermark
    committer: OffsetCommitter
    floor: int | None = None
    dirty: bool = False

    def committable_offset(self) -> int | None:
        """Return the offset safe to commit now, or ``None`` when there is none.

        ``None`` means either nothing has completed yet, or the watermark is
        still strictly below the floor — a recovery replay catching up to the
        group offset it must never rewind.
        """
        offset = self.watermark.commit_offset
        if offset is None:
            return None
        if self.floor is not None and offset < self.floor:
            return None
        return offset


@dataclass(slots=True)
class _TrackedMessage:
    """Pending Kafka message that still requires downstream completion."""

    topic: str
    partition: int
    offset: int
    pending: int


@dataclass(slots=True)
class _PartitionWatermark:
    """Gap-tolerant commit watermark for one Kafka topic-partition.

    Registered offsets are kept in arrival order (broker order within a
    partition). The watermark advances past the head of that queue as heads
    complete; offsets that were never registered — transactional control
    records, compaction gaps — are simply never waited on.
    """

    topic: str
    partition: int
    commit_offset: int | None = None
    _order: deque[int] = field(default_factory=deque)
    _done: set[int] = field(default_factory=set)

    def register(self, offset: int) -> None:
        """Register one offset as in-flight for this partition."""
        self._order.append(offset)

    def complete(self, offset: int) -> bool:
        """Mark one offset done; return True when the watermark advanced."""
        self._done.add(offset)
        advanced = False
        while self._order and self._order[0] in self._done:
            head = self._order.popleft()
            self._done.discard(head)
            self.commit_offset = head + 1
            advanced = True
        return advanced


__all__ = ["CommitCompletionPort", "KafkaCommitTracker", "OffsetCommitter"]
