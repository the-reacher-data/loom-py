"""Kafka commit tracker for the streaming Bytewax runtime."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from confluent_kafka import TopicPartition

from loom.streaming.kafka._record import KafkaRecord
from loom.streaming.kafka.client._consumer import KafkaConsumerClient


class KafkaCommitTracker:
    """Track per-offset completion and commit Kafka offsets once safe."""

    def __init__(self) -> None:
        self._consumer: KafkaConsumerClient | None = None
        self._messages: dict[str, _TrackedMessage] = {}
        self._watermarks: dict[tuple[str, int], _PartitionWatermark] = {}
        self._lock = threading.Lock()

    def bind(self, consumer: object) -> None:
        """Bind the source consumer that will be committed."""
        with self._lock:
            self._consumer = consumer  # type: ignore[assignment]

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
        """Mark one logical branch as completed and commit when all finish."""
        key = f"{topic}:{partition}:{offset}"
        consumer: KafkaConsumerClient | None = None
        commit_partitions: list[TopicPartition] = []
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
            commit_partition = watermark.complete(state.offset)
            if commit_partition is not None:
                commit_partitions.append(commit_partition)
            consumer = self._consumer
        if consumer is not None and commit_partitions:
            consumer.commit_offset(commit_partitions)


@dataclass(slots=True)
class _TrackedMessage:
    """Pending Kafka message that still requires downstream completion."""

    topic: str
    partition: int
    offset: int
    pending: int


@dataclass(slots=True)
class _PartitionWatermark:
    """Contiguous watermark for one Kafka topic-partition."""

    topic: str
    partition: int
    next_offset: int | None = None
    pending: set[int] = field(default_factory=set)
    ready: set[int] = field(default_factory=set)

    def register(self, offset: int) -> None:
        """Register one offset as in-flight for this partition."""
        if self.next_offset is None or offset < self.next_offset:
            self.next_offset = offset
        self.pending.add(offset)

    def complete(self, offset: int) -> TopicPartition | None:
        """Advance the contiguous watermark when one offset completes."""
        if self.next_offset is None:
            return None
        self.pending.discard(offset)
        self.ready.add(offset)
        advanced = False
        while self.next_offset in self.ready:
            self.ready.remove(self.next_offset)
            self.next_offset += 1
            advanced = True
        if not advanced:
            return None
        return TopicPartition(self.topic, self.partition, self.next_offset)


__all__ = ["KafkaCommitTracker"]
