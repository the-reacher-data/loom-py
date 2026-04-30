"""Runtime Bytewax source and sink adapters for Loom streaming flows."""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from bytewax.inputs import SimplePollingSource
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from confluent_kafka import TopicPartition

from loom.streaming.compiler._plan import CompiledSink, CompiledSource
from loom.streaming.core._errors import ErrorKind
from loom.streaming.core._message import Message
from loom.streaming.core._typing import StreamPayload
from loom.streaming.kafka._codec import MsgspecCodec
from loom.streaming.kafka._errors import KafkaDeliveryError
from loom.streaming.kafka._message import MessageDescriptor
from loom.streaming.kafka._record import KafkaRecord
from loom.streaming.kafka.client._consumer import KafkaConsumerClient
from loom.streaming.kafka.client._producer import KafkaProducerClient
from loom.streaming.kafka.message._producer import KafkaMessageProducer
from loom.streaming.nodes._boundary import PartitionPolicy

logger = logging.getLogger(__name__)


class _KafkaPollingSource(SimplePollingSource[KafkaRecord[bytes], None]):
    """Poll Kafka records from one worker using the project Kafka client."""

    def __init__(
        self,
        source: CompiledSource,
        commit_tracker: _KafkaCommitTracker | None = None,
    ) -> None:
        self._poll_timeout_ms = source.settings.poll_timeout_ms
        super().__init__(interval=timedelta(milliseconds=self._poll_timeout_ms))
        self._consumer = KafkaConsumerClient(source.settings)
        self._commit_tracker = commit_tracker
        if self._commit_tracker is not None:
            self._commit_tracker.bind(self._consumer)

    def next_item(self) -> KafkaRecord[bytes]:
        record = self._consumer.poll(self._poll_timeout_ms)
        if record is None:
            raise SimplePollingSource.Retry(timedelta(milliseconds=0))
        if self._commit_tracker is not None:
            self._commit_tracker.register_record(record)
        return record

    def bind_commit_tracker(self, commit_tracker: _KafkaCommitTracker | None) -> None:
        """Bind or clear the source commit tracker."""
        self._commit_tracker = commit_tracker
        if self._commit_tracker is not None:
            self._commit_tracker.bind(self._consumer)

    def close(self) -> None:
        self._consumer.close()


class _KafkaMessageSinkPartition(StatelessSinkPartition[Message[StreamPayload]]):
    """Write Loom messages to one Kafka output topic."""

    def __init__(
        self,
        sink: CompiledSink,
        commit_tracker: _KafkaCommitTracker | None = None,
    ) -> None:
        self._sink = sink
        self._dlq_topic = sink.dlq_topic
        self._producer = KafkaProducerClient(sink.settings)
        self._message_producer: KafkaMessageProducer[StreamPayload] = KafkaMessageProducer(
            self._producer,
            MsgspecCodec(),
        )
        self._commit_tracker = commit_tracker

    def write_batch(self, items: list[Message[StreamPayload]]) -> None:
        try:
            for message in items:
                descriptor = MessageDescriptor(
                    message_type=message.meta.message_type or self._sink.topic,
                    message_version=message.meta.message_version or 1,
                )
                key = _resolve_partition_key(message, self._sink.partition_policy)
                self._message_producer.send(
                    topic=self._sink.topic,
                    payload=message.payload,
                    descriptor=descriptor,
                    key=key,
                    headers=message.meta.headers,
                    correlation_id=message.meta.correlation_id,
                    causation_id=message.meta.causation_id,
                    trace_id=message.meta.trace_id,
                    produced_at_ms=message.meta.produced_at_ms,
                )
            self._producer.flush()
            self._commit_items(items)
        except KafkaDeliveryError as exc:
            if self._dlq_topic is not None:
                _send_batch_to_dlq(self._message_producer, self._dlq_topic, items, exc)
                self._commit_items(items)
            else:
                raise

    def close(self) -> None:
        self._message_producer.close()

    def bind_commit_tracker(self, tracker: _KafkaCommitTracker | None) -> None:
        """Bind a commit tracker used to ack source offsets after success."""
        self._commit_tracker = tracker

    def _commit_items(self, items: list[Message[StreamPayload]]) -> None:
        tracker = self._commit_tracker
        if tracker is None:
            return
        for message in items:
            tracker.complete(message.meta.message_id)


class _KafkaMessageSink(DynamicSink[Message[StreamPayload]]):
    """Build Kafka sink partitions for the runtime output topic."""

    def __init__(self, sink: CompiledSink) -> None:
        self._sink = sink
        self._commit_tracker: _KafkaCommitTracker | None = None

    def build(
        self,
        step_id: str,
        worker_index: int,
        worker_count: int,
    ) -> StatelessSinkPartition[Message[StreamPayload]]:
        return _KafkaMessageSinkPartition(self._sink, self._commit_tracker)

    def bind_commit_tracker(self, tracker: _KafkaCommitTracker | None) -> None:
        """Bind a commit tracker used by all partitions built from this sink."""
        self._commit_tracker = tracker


def build_runtime_source(
    source: CompiledSource,
    commit_tracker: _KafkaCommitTracker | None = None,
) -> _KafkaPollingSource:
    """Build the runtime source for one compiled Kafka input."""
    return _KafkaPollingSource(source, commit_tracker)


def build_runtime_sink(
    sink: CompiledSink,
    commit_tracker: _KafkaCommitTracker | None = None,
) -> _KafkaMessageSink:
    """Build the runtime sink for one compiled Kafka output."""
    runtime_sink = _KafkaMessageSink(sink)
    runtime_sink.bind_commit_tracker(commit_tracker)
    return runtime_sink


def build_runtime_error_sinks(
    error_routes: dict[ErrorKind, CompiledSink],
    commit_tracker: _KafkaCommitTracker | None = None,
) -> dict[ErrorKind, _KafkaMessageSink]:
    """Build runtime sinks for explicit error routes."""
    return {kind: build_runtime_sink(sink, commit_tracker) for kind, sink in error_routes.items()}


def build_runtime_terminal_sinks(
    terminal_sinks: dict[tuple[int, ...], CompiledSink],
    commit_tracker: _KafkaCommitTracker | None = None,
) -> dict[tuple[int, ...], _KafkaMessageSink]:
    """Build runtime sinks for terminal branch outputs."""
    return {path: build_runtime_sink(sink, commit_tracker) for path, sink in terminal_sinks.items()}


def build_inline_sink_partition(
    sink: CompiledSink,
    commit_tracker: _KafkaCommitTracker | None = None,
) -> _KafkaMessageSinkPartition:
    """Build a sink partition for direct (non-Bytewax-graph) message writing.

    Used by ``WithAsync(process=...)`` to write messages directly to Kafka
    from within the async concurrent execution, bypassing the Bytewax output
    wiring layer.

    Args:
        sink: Compiled Kafka sink configuration.

    Returns:
        A ready-to-write sink partition.
    """
    return _KafkaMessageSinkPartition(sink, commit_tracker)


def build_commit_tracker(source: CompiledSource) -> _KafkaCommitTracker | None:
    """Build a commit tracker when explicit source commits are required."""
    if source.settings.enable_auto_commit:
        return None
    return _KafkaCommitTracker()


class _KafkaCommitTracker:
    """Track per-offset completion and commit Kafka offsets once safe."""

    def __init__(self) -> None:
        self._consumer: Any | None = None
        self._messages: dict[str, _TrackedMessage] = {}
        self._watermarks: dict[tuple[str, int], _PartitionWatermark] = {}
        self._lock = threading.Lock()

    def bind(self, consumer: object) -> None:
        """Bind the source consumer that will be committed."""
        with self._lock:
            self._consumer = consumer

    def register_record(self, record: KafkaRecord[bytes]) -> None:
        """Register one newly polled Kafka record for offset tracking."""
        if record.partition is None or record.offset is None:
            return
        message_id = _record_id(record)
        with self._lock:
            if message_id in self._messages:
                return
            self._messages[message_id] = _TrackedMessage(
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

    def fork(self, message_id: str, extra_outputs: int) -> None:
        """Increase the number of expected completions for one message."""
        if extra_outputs <= 0:
            return
        with self._lock:
            state = self._messages.get(message_id)
            if state is None:
                return
            state.pending += extra_outputs

    def complete(self, message_id: str) -> None:
        """Mark one logical branch as completed and commit when all finish."""
        consumer: KafkaConsumerClient | None = None
        commit_partitions: list[TopicPartition] = []
        with self._lock:
            state = self._messages.get(message_id)
            if state is None:
                return
            state.pending -= 1
            if state.pending > 0:
                return
            self._messages.pop(message_id, None)
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
    committed: bool = False

    def register(self, offset: int) -> None:
        """Register one offset as in-flight for this partition."""
        if self.next_offset is None or (not self.committed and offset < self.next_offset):
            self.next_offset = offset
        self.pending.add(offset)

    def complete(self, offset: int) -> TopicPartition | None:
        """Advance the contiguous watermark when one offset completes."""
        if self.next_offset is None:
            return None
        self.pending.discard(offset)
        self.ready.add(offset)
        advanced = False
        while self.next_offset in self.ready and self.next_offset not in self.pending:
            self.ready.remove(self.next_offset)
            self.next_offset += 1
            advanced = True
        if not advanced:
            return None
        self.committed = True
        return TopicPartition(self.topic, self.partition, self.next_offset)


def _resolve_partition_key(
    message: Message[StreamPayload],
    partition_policy: PartitionPolicy[Any] | None,
) -> bytes | str | None:
    """Resolve the outgoing Kafka key for one message.

    The transport key is preserved when the incoming message already carries
    one and the policy does not explicitly allow repartitioning. When no
    transport key is present, the declared policy strategy may derive one.

    Args:
        message: Transport-neutral message to publish.
        partition_policy: Optional output partitioning policy.

    Returns:
        Kafka key to use for the outgoing record, or ``None``.
    """
    if partition_policy is None:
        return message.meta.key

    incoming_key = message.meta.key
    policy_key = partition_policy.strategy.partition_key(message)

    if incoming_key is None:
        return policy_key
    if partition_policy.allow_repartition:
        return policy_key if policy_key is not None else incoming_key
    return incoming_key


def _record_id(record: KafkaRecord[bytes]) -> str:
    """Derive a stable logical id for one raw Kafka record."""
    if record.partition is not None and record.offset is not None:
        return f"{record.topic}:{record.partition}:{record.offset}"
    if record.key is not None:
        return f"{record.topic}:{record.key!s}"
    if record.timestamp_ms is not None:
        return f"{record.topic}:{record.timestamp_ms}"
    return record.topic


def _send_batch_to_dlq(
    producer: KafkaMessageProducer[StreamPayload],
    dlq_topic: str,
    messages: list[Message[StreamPayload]],
    exc: KafkaDeliveryError,
) -> None:
    """Best-effort: send all messages in *messages* to the DLQ topic.

    Called when a batch delivery fails and a DLQ is configured. Each message
    is re-sent to *dlq_topic* with an ``x-dlq-error`` header. Failures during
    DLQ delivery are logged and suppressed — a crashing DLQ is worse than a
    missing one.
    """
    error_bytes = str(exc).encode("utf-8")
    for message in messages:
        try:
            descriptor = MessageDescriptor(
                message_type=message.meta.message_type or dlq_topic,
                message_version=message.meta.message_version or 1,
            )
            headers = {**message.meta.headers, "x-dlq-error": error_bytes}
            producer.send(
                topic=dlq_topic,
                payload=message.payload,
                descriptor=descriptor,
                headers=headers,
                correlation_id=message.meta.correlation_id,
                causation_id=message.meta.causation_id,
                trace_id=message.meta.trace_id,
                produced_at_ms=message.meta.produced_at_ms,
            )
        except Exception:
            logger.warning("dlq_send_failed", extra={"dlq_topic": dlq_topic})
