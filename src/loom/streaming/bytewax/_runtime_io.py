"""Runtime Bytewax source and sink adapters for Loom streaming flows."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Generic, TypeAlias, TypeVar, cast

from bytewax.inputs import SimplePollingSource
from bytewax.outputs import DynamicSink, StatelessSinkPartition

from loom.core.tracing import generate_trace_id
from loom.streaming.bytewax._commit_tracker import KafkaCommitTracker
from loom.streaming.bytewax._dlq import (
    send_batch_to_dlq,
    send_decode_error_batch_to_dlq,
    send_error_batch_to_dlq,
)
from loom.streaming.compiler._plan import CompiledSink, CompiledSource
from loom.streaming.core._errors import ErrorEnvelope, ErrorKind
from loom.streaming.core._message import Message
from loom.streaming.core._typing import StreamPayload
from loom.streaming.kafka._codec import MsgspecCodec
from loom.streaming.kafka._errors import KafkaDeliveryError
from loom.streaming.kafka._message import (
    HEADER_CAUSATION_ID,
    HEADER_CORRELATION_ID,
    HEADER_TRACE_ID,
    MessageDescriptor,
)
from loom.streaming.kafka._record import KafkaRecord
from loom.streaming.kafka._wire import DecodeError
from loom.streaming.kafka.client._consumer import KafkaConsumerClient
from loom.streaming.kafka.client._producer import KafkaProducerClient
from loom.streaming.kafka.message._producer import KafkaMessageProducer
from loom.streaming.nodes._boundary import PartitionPolicy

logger = logging.getLogger(__name__)
ItemT = TypeVar("ItemT")
PayloadT = TypeVar("PayloadT", bound=StreamPayload)


@dataclass(frozen=True)
class _KafkaSendRequest(Generic[PayloadT]):
    """Resolved Kafka send parameters for one runtime item."""

    payload: PayloadT
    descriptor: MessageDescriptor
    key: bytes | str | None
    headers: dict[str, bytes]
    correlation_id: str | None
    parent_trace_id: str | None
    causation_id: str | None
    trace_id: str | None
    produced_at_ms: int | None


def _write_kafka_batch(
    *,
    sink: CompiledSink,
    producer: KafkaProducerClient,
    message_producer: KafkaMessageProducer[PayloadT],
    items: list[ItemT],
    item_to_send: Callable[[ItemT], _KafkaSendRequest[PayloadT]],
    item_to_commit: Callable[[ItemT], tuple[str | None, int | None, int | None]] | None,
    dlq_sender: Callable[
        [KafkaMessageProducer[PayloadT], str, list[ItemT], KafkaDeliveryError], None
    ]
    | None,
    commit_tracker: KafkaCommitTracker | None,
) -> None:
    try:
        for item in items:
            request = item_to_send(item)
            message_producer.send(
                topic=sink.topic,
                payload=request.payload,
                descriptor=request.descriptor,
                key=request.key,
                headers=request.headers,
                correlation_id=request.correlation_id,
                parent_trace_id=request.parent_trace_id,
                causation_id=request.causation_id,
                trace_id=request.trace_id,
                produced_at_ms=request.produced_at_ms,
            )
        producer.flush()
        _commit_runtime_items(items, item_to_commit, commit_tracker)
    except KafkaDeliveryError as exc:
        if sink.dlq_topic is not None and dlq_sender is not None:
            dlq_sender(message_producer, sink.dlq_topic, items, exc)
            _commit_runtime_items(items, item_to_commit, commit_tracker)
            return
        raise


def _commit_runtime_items(
    items: list[ItemT],
    item_to_commit: Callable[[ItemT], tuple[str | None, int | None, int | None]] | None,
    commit_tracker: KafkaCommitTracker | None,
) -> None:
    if commit_tracker is None or item_to_commit is None:
        return
    for item in items:
        topic, partition, offset = item_to_commit(item)
        if topic is not None and partition is not None and offset is not None:
            commit_tracker.complete(topic, partition, offset)


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


class _KafkaSinkPartitionBase:
    """Shared Kafka producer lifecycle and commit-tracker management."""

    def __init__(
        self,
        sink: CompiledSink,
        commit_tracker: KafkaCommitTracker | None = None,
    ) -> None:
        self._sink = sink
        self._producer = KafkaProducerClient(sink.settings)
        self._message_producer: KafkaMessageProducer[Any] = KafkaMessageProducer(
            self._producer, MsgspecCodec()
        )
        self._commit_tracker = commit_tracker

    def close(self) -> None:
        self._message_producer.close()

    def bind_commit_tracker(self, tracker: KafkaCommitTracker | None) -> None:
        self._commit_tracker = tracker


class _KafkaMessageSinkPartition(
    _KafkaSinkPartitionBase, StatelessSinkPartition[Message[StreamPayload]]
):
    """Write typed runtime messages to Kafka."""

    _message_producer: KafkaMessageProducer[StreamPayload]

    def write_batch(self, items: list[Message[StreamPayload]]) -> None:
        _write_kafka_batch(
            sink=self._sink,
            producer=self._producer,
            message_producer=self._message_producer,
            items=items,
            item_to_send=lambda msg: _message_to_send_with_policy(msg, self._sink.partition_policy),
            item_to_commit=_message_to_commit,
            dlq_sender=send_batch_to_dlq,
            commit_tracker=self._commit_tracker,
        )


class _KafkaErrorEnvelopeSinkPartition(
    _KafkaSinkPartitionBase, StatelessSinkPartition[ErrorEnvelope[StreamPayload]]
):
    """Write Loom error envelopes to Kafka."""

    _message_producer: KafkaMessageProducer[ErrorEnvelope[StreamPayload]]

    def write_batch(self, items: list[ErrorEnvelope[StreamPayload]]) -> None:
        _write_kafka_batch(
            sink=self._sink,
            producer=self._producer,
            message_producer=self._message_producer,
            items=items,
            item_to_send=lambda envelope: _error_item_to_send(
                envelope, self._sink.partition_policy
            ),
            item_to_commit=_error_item_to_commit,
            dlq_sender=send_error_batch_to_dlq,
            commit_tracker=self._commit_tracker,
        )


class _KafkaDecodeErrorSinkPartition(_KafkaSinkPartitionBase, StatelessSinkPartition[DecodeError]):
    """Write Kafka wire decode failures to Kafka."""

    _message_producer: KafkaMessageProducer[DecodeError]

    def write_batch(self, items: list[DecodeError]) -> None:
        _write_kafka_batch(
            sink=self._sink,
            producer=self._producer,
            message_producer=self._message_producer,
            items=items,
            item_to_send=lambda error: _decode_error_to_send(error, self._sink.partition_policy),
            item_to_commit=_decode_error_to_commit,
            dlq_sender=send_decode_error_batch_to_dlq,
            commit_tracker=self._commit_tracker,
        )


class _KafkaDynamicSinkBase:
    """Shared init and commit-tracker management for Kafka dynamic sinks."""

    def __init__(self, sink: CompiledSink) -> None:
        self._sink = sink
        self._commit_tracker: KafkaCommitTracker | None = None

    def bind_commit_tracker(self, tracker: KafkaCommitTracker | None) -> None:
        """Bind a commit tracker used by all partitions built from this sink."""
        self._commit_tracker = tracker


class _KafkaMessageSink(_KafkaDynamicSinkBase, DynamicSink[Message[StreamPayload]]):
    """Build Kafka sink partitions for the runtime output topic."""

    def build(
        self,
        step_id: str,
        worker_index: int,
        worker_count: int,
    ) -> _KafkaMessageSinkPartition:
        del step_id, worker_index, worker_count
        return _KafkaMessageSinkPartition(self._sink, self._commit_tracker)


class _KafkaErrorEnvelopeSink(_KafkaDynamicSinkBase, DynamicSink[ErrorEnvelope[StreamPayload]]):
    """Build Kafka sink partitions for routed error envelopes."""

    def build(
        self,
        step_id: str,
        worker_index: int,
        worker_count: int,
    ) -> _KafkaErrorEnvelopeSinkPartition:
        del step_id, worker_index, worker_count
        return _KafkaErrorEnvelopeSinkPartition(self._sink, self._commit_tracker)


class _KafkaDecodeErrorSink(_KafkaDynamicSinkBase, DynamicSink[DecodeError]):
    """Build Kafka sink partitions for wire decode failures."""

    def build(
        self,
        step_id: str,
        worker_index: int,
        worker_count: int,
    ) -> _KafkaDecodeErrorSinkPartition:
        del step_id, worker_index, worker_count
        return _KafkaDecodeErrorSinkPartition(self._sink, self._commit_tracker)


_ErrorSink: TypeAlias = _KafkaErrorEnvelopeSink | _KafkaDecodeErrorSink


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
) -> dict[ErrorKind, _ErrorSink]:
    """Build runtime sinks for explicit error routes.

    ``WIRE`` errors carry ``DecodeError`` items and require ``_KafkaDecodeErrorSink``.
    All other kinds carry ``ErrorEnvelope`` items and use ``_KafkaErrorEnvelopeSink``.
    Keeping these as distinct typed sinks is the invariant that prevents routing a
    ``DecodeError`` into a function expecting an ``ErrorEnvelope``.
    """
    runtime_sinks: dict[ErrorKind, _ErrorSink] = {}
    for kind, sink in error_routes.items():
        if kind is ErrorKind.WIRE:
            runtime_sinks[kind] = _KafkaDecodeErrorSink(sink)
        else:
            runtime_sinks[kind] = _KafkaErrorEnvelopeSink(sink)
    for error_sink in runtime_sinks.values():
        error_sink.bind_commit_tracker(commit_tracker)
    return runtime_sinks


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
        commit_tracker: Optional commit tracker for manual offset acknowledgment.

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


def _message_to_send(message: Message[StreamPayload]) -> _KafkaSendRequest[StreamPayload]:
    return _message_to_send_with_policy(message, None)


def _message_to_send_with_policy(
    message: Message[StreamPayload],
    partition_policy: PartitionPolicy[Any] | None,
) -> _KafkaSendRequest[StreamPayload]:
    return _KafkaSendRequest(
        payload=message.payload,
        descriptor=MessageDescriptor(
            message_type=message.meta.message_type or "loom.streaming.message",
            message_version=message.meta.message_version or 1,
        ),
        key=_resolve_partition_key(message, partition_policy),
        headers=message.meta.headers,
        correlation_id=message.meta.correlation_id,
        parent_trace_id=message.meta.trace_id,
        causation_id=message.meta.causation_id,
        trace_id=generate_trace_id(),
        produced_at_ms=message.meta.produced_at_ms,
    )


def _message_to_commit(
    message: Message[StreamPayload],
) -> tuple[str | None, int | None, int | None]:
    return message.meta.topic, message.meta.partition, message.meta.offset


def _error_item_to_send(
    item: ErrorEnvelope[StreamPayload],
    partition_policy: PartitionPolicy[Any] | None,
) -> _KafkaSendRequest[ErrorEnvelope[StreamPayload]]:
    original = item.original_message
    headers: dict[str, bytes] = {
        "x-error-kind": item.kind.value.encode("utf-8"),
        "x-error-reason": item.reason.encode("utf-8"),
    }
    descriptor = MessageDescriptor(
        message_type=f"loom.streaming.error.{item.kind.value}",
        message_version=1,
    )
    if original is None:
        return _KafkaSendRequest(
            payload=item,
            descriptor=descriptor,
            key=None,
            headers=headers,
            correlation_id=None,
            parent_trace_id=None,
            causation_id=None,
            trace_id=generate_trace_id(),
            produced_at_ms=None,
        )
    key = _resolve_partition_key(cast(Message[StreamPayload], original), partition_policy)
    return _KafkaSendRequest(
        payload=item,
        descriptor=descriptor,
        key=key,
        headers={**original.meta.headers, **headers},
        correlation_id=original.meta.correlation_id,
        parent_trace_id=original.meta.trace_id,
        causation_id=original.meta.causation_id,
        trace_id=generate_trace_id(),
        produced_at_ms=original.meta.produced_at_ms,
    )


def _error_item_to_commit(
    item: ErrorEnvelope[StreamPayload],
) -> tuple[str | None, int | None, int | None]:
    original = item.original_message
    if original is None:
        return (None, None, None)
    return original.meta.topic, original.meta.partition, original.meta.offset


def _decode_str_header(headers: dict[str, bytes], key: str) -> str | None:
    raw = headers.get(key)
    return raw.decode() if raw is not None else None


def _decode_error_to_send(
    item: DecodeError,
    partition_policy: PartitionPolicy[Any] | None,
) -> _KafkaSendRequest[DecodeError]:
    del partition_policy
    return _KafkaSendRequest(
        payload=item,
        descriptor=MessageDescriptor(
            message_type=DecodeError.loom_message_type(), message_version=1
        ),
        key=item.key,
        headers={
            **item.headers,
            "x-error-kind": item.error.kind.value.encode("utf-8"),
            "x-error-reason": item.error.reason.encode("utf-8"),
        },
        correlation_id=_decode_str_header(item.headers, HEADER_CORRELATION_ID),
        parent_trace_id=_decode_str_header(item.headers, HEADER_TRACE_ID),
        causation_id=_decode_str_header(item.headers, HEADER_CAUSATION_ID),
        trace_id=generate_trace_id(),
        produced_at_ms=item.timestamp_ms,
    )


def _decode_error_to_commit(
    item: DecodeError,
) -> tuple[str | None, int | None, int | None]:
    return item.topic, item.partition, item.offset
