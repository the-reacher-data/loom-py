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
  advances are accumulated per partition and committed by ``flush()`` — one
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


class KafkaCommitTracker:
    """Track per-offset completion and commit Kafka offsets once safe."""

    def __init__(self) -> None:
        self._default_committer: OffsetCommitter | None = None
        self._committers: dict[tuple[str, int], OffsetCommitter] = {}
        self._messages: dict[str, _TrackedMessage] = {}
        self._watermarks: dict[tuple[str, int], _PartitionWatermark] = {}
        self._floors: dict[tuple[str, int], int] = {}
        self._dirty: set[tuple[str, int]] = set()
        self._lock = threading.Lock()

    def bind(self, consumer: object) -> None:
        """Bind the default committer used for partitions without their own.

        Kept for single-consumer sources and test harnesses; partitioned
        sources should prefer :meth:`bind_partition`.
        """
        with self._lock:
            self._default_committer = consumer  # type: ignore[assignment]

    def bind_partition(self, topic: str, partition: int, committer: OffsetCommitter) -> None:
        """Bind the committer that owns one topic partition."""
        with self._lock:
            self._committers[(topic, partition)] = committer

    def set_floor(self, topic: str, partition: int, offset: int | None) -> None:
        """Set the commit floor for one partition.

        Commits at or below the floor are suppressed so a recovery replay can
        never rewind the consumer-group watermark. ``None`` clears the floor.
        """
        with self._lock:
            if offset is None:
                self._floors.pop((topic, partition), None)
            else:
                self._floors[(topic, partition)] = offset

    def seed_watermark(self, topic: str, partition: int, offset: int | None) -> None:
        """Seed a partition's commit position with the group's committed offset.

        Gives idle partitions (no traffic this run) a watermark the retention
        keep-alive can re-commit; re-committing the same group offset is
        idempotent and never rewinds.
        """
        if offset is None:
            return
        with self._lock:
            watermark = self._watermarks.setdefault(
                (topic, partition),
                _PartitionWatermark(topic=topic, partition=partition),
            )
            if watermark.commit_offset is None:
                watermark.commit_offset = offset

    def reset_partition(self, topic: str, partition: int) -> None:
        """Drop all in-flight state for one partition.

        Called when a partition is (re)built so a replay after resume starts
        from a clean watermark instead of colliding with stale offsets.
        """
        with self._lock:
            self._watermarks.pop((topic, partition), None)
            self._dirty.discard((topic, partition))
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
            if key in self._messages:
                return
            self._messages[key] = _TrackedMessage(
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                pending=1,
            )
            watermark = self._watermarks.setdefault(
                (record.topic, record.partition),
                _PartitionWatermark(topic=record.topic, partition=record.partition),
            )
            watermark.register(record.offset)

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
            watermark = self._watermarks.get((state.topic, state.partition))
            if watermark is None:
                return
            if watermark.complete(state.offset):
                self._dirty.add((state.topic, state.partition))

    def flush(
        self,
        topic: str | None = None,
        partition: int | None = None,
        *,
        force: bool = False,
        synchronous: bool = True,
    ) -> list[TopicPartition]:
        """Commit accumulated watermarks, coalesced per partition.

        Must be invoked only from the thread that owns the flushed partition
        (the source partition's poll loop, or close): that single-writer rule
        is what makes out-of-order group commits impossible.

        Args:
            topic: Restrict the flush to one topic (with ``partition``).
            partition: Restrict the flush to one partition of ``topic``.
            force: Commit the current watermark even when it did not advance
                since the last flush — used as a retention keep-alive for
                member-less consumer groups.
            synchronous: ``False`` lets librdkafka coalesce the commit in the
                background (hot path); ``True`` blocks until acknowledged
                (close/shutdown).

        Returns:
            The topic-partition offsets handed to the committer.

        Raises:
            KafkaCommitError: Propagated from the committer when the broker
                commit fails; the affected partitions are re-marked dirty so
                the next flush retries them.
        """
        plan: list[tuple[OffsetCommitter, TopicPartition]] = []
        with self._lock:
            for key in self._flush_targets(topic, partition, force=force):
                watermark = self._watermarks.get(key)
                if watermark is None or watermark.commit_offset is None:
                    continue
                floor = self._floors.get(key)
                if floor is not None and watermark.commit_offset < floor:
                    self._dirty.discard(key)
                    continue
                committer = self._committers.get(key) or self._default_committer
                if committer is None:
                    continue
                plan.append((committer, TopicPartition(key[0], key[1], watermark.commit_offset)))
                self._dirty.discard(key)
        committed: list[TopicPartition] = []
        try:
            for committer, target in plan:
                committer.commit_offset([target], asynchronous=not synchronous)
                committed.append(target)
        except Exception:
            with self._lock:
                for _, target in plan[len(committed) :]:
                    self._dirty.add((target.topic, target.partition))
            raise
        return committed

    def _flush_targets(
        self,
        topic: str | None,
        partition: int | None,
        *,
        force: bool,
    ) -> list[tuple[str, int]]:
        """Resolve which partitions this flush call should consider."""
        if topic is not None and partition is not None:
            key = (topic, partition)
            if force or key in self._dirty:
                return [key]
            return []
        if force:
            return list(self._watermarks)
        return list(self._dirty)


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


__all__ = ["KafkaCommitTracker", "OffsetCommitter"]
