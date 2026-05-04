"""Runtime Bytewax source and sink adapters for Loom streaming flows."""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from bytewax.inputs import SimplePollingSource
from bytewax.outputs import DynamicSink, StatelessSinkPartition

from loom.streaming.bytewax._commit_tracker import KafkaCommitTracker
from loom.streaming.bytewax._dlq import send_batch_to_dlq
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
        commit_tracker: KafkaCommitTracker | None = None,
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

    def bind_commit_tracker(self, commit_tracker: KafkaCommitTracker | None) -> None:
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
        commit_tracker: KafkaCommitTracker | None = None,
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
                send_batch_to_dlq(self._message_producer, self._dlq_topic, items, exc)
                self._commit_items(items)
            else:
                raise

    def close(self) -> None:
        self._message_producer.close()

    def bind_commit_tracker(self, tracker: KafkaCommitTracker | None) -> None:
        """Bind a commit tracker used to ack source offsets after success."""
        self._commit_tracker = tracker

    def _commit_items(self, items: list[Message[StreamPayload]]) -> None:
        tracker = self._commit_tracker
        if tracker is None:
            return
        for message in items:
            t, p, o = message.meta.topic, message.meta.partition, message.meta.offset
            if t is not None and p is not None and o is not None:
                tracker.complete(t, p, o)


class _KafkaMessageSink(DynamicSink[Message[StreamPayload]]):
    """Build Kafka sink partitions for the runtime output topic."""

    def __init__(self, sink: CompiledSink) -> None:
        self._sink = sink
        self._commit_tracker: KafkaCommitTracker | None = None

    def build(
        self,
        step_id: str,
        worker_index: int,
        worker_count: int,
    ) -> StatelessSinkPartition[Message[StreamPayload]]:
        del step_id, worker_index, worker_count
        return _KafkaMessageSinkPartition(self._sink, self._commit_tracker)

    def bind_commit_tracker(self, tracker: KafkaCommitTracker | None) -> None:
        """Bind a commit tracker used by all partitions built from this sink."""
        self._commit_tracker = tracker


def build_runtime_source(
    source: CompiledSource,
    commit_tracker: KafkaCommitTracker | None = None,
) -> _KafkaPollingSource:
    """Build the runtime source for one compiled Kafka input."""
    return _KafkaPollingSource(source, commit_tracker)


def build_runtime_sink(
    sink: CompiledSink,
    commit_tracker: KafkaCommitTracker | None = None,
) -> _KafkaMessageSink:
    """Build the runtime sink for one compiled Kafka output."""
    runtime_sink = _KafkaMessageSink(sink)
    runtime_sink.bind_commit_tracker(commit_tracker)
    return runtime_sink


def build_runtime_error_sinks(
    error_routes: dict[ErrorKind, CompiledSink],
    commit_tracker: KafkaCommitTracker | None = None,
) -> dict[ErrorKind, _KafkaMessageSink]:
    """Build runtime sinks for explicit error routes."""
    return {kind: build_runtime_sink(sink, commit_tracker) for kind, sink in error_routes.items()}


def build_runtime_terminal_sinks(
    terminal_sinks: dict[tuple[int, ...], CompiledSink],
    commit_tracker: KafkaCommitTracker | None = None,
) -> dict[tuple[int, ...], _KafkaMessageSink]:
    """Build runtime sinks for terminal branch outputs."""
    return {path: build_runtime_sink(sink, commit_tracker) for path, sink in terminal_sinks.items()}


def build_inline_sink_partition(
    sink: CompiledSink,
    commit_tracker: KafkaCommitTracker | None = None,
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


def build_commit_tracker(source: CompiledSource) -> KafkaCommitTracker | None:
    """Build a commit tracker when explicit source commits are required."""
    if source.settings.enable_auto_commit:
        return None
    return KafkaCommitTracker()


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
