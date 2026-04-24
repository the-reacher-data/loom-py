"""Runtime Bytewax source and sink adapters for Loom streaming flows."""

from __future__ import annotations

from datetime import timedelta
from typing import Any, cast

from bytewax.inputs import SimplePollingSource
from bytewax.outputs import DynamicSink, StatelessSinkPartition

from loom.streaming.compiler._plan import CompiledSink, CompiledSource
from loom.streaming.core._errors import ErrorKind
from loom.streaming.core._message import Message
from loom.streaming.core._typing import StreamPayload
from loom.streaming.kafka._codec import MsgspecCodec
from loom.streaming.kafka._message import MessageDescriptor
from loom.streaming.kafka._record import KafkaRecord
from loom.streaming.kafka.client._consumer import KafkaConsumerClient
from loom.streaming.kafka.client._producer import KafkaProducerClient
from loom.streaming.kafka.message._producer import KafkaMessageProducer
from loom.streaming.nodes._boundary import PartitionPolicy


class _KafkaPollingSource(SimplePollingSource[KafkaRecord[bytes], None]):
    """Poll Kafka records from one worker using the project Kafka client."""

    def __init__(self, source: CompiledSource) -> None:
        super().__init__(interval=timedelta(milliseconds=1))
        self._consumer = KafkaConsumerClient(source.settings)

    def next_item(self) -> KafkaRecord[bytes]:
        return cast(KafkaRecord[bytes], self._consumer.poll(0))

    def close(self) -> None:
        self._consumer.close()


class _KafkaMessageSinkPartition(StatelessSinkPartition[Message[StreamPayload]]):
    """Write Loom messages to one Kafka output topic."""

    def __init__(self, sink: CompiledSink) -> None:
        self._sink = sink
        self._producer = KafkaProducerClient(sink.settings)
        self._message_producer: KafkaMessageProducer[StreamPayload] = KafkaMessageProducer(
            self._producer,
            MsgspecCodec(),
        )

    def write_batch(self, items: list[Message[StreamPayload]]) -> None:
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

    def close(self) -> None:
        self._message_producer.close()


class _KafkaMessageSink(DynamicSink[Message[StreamPayload]]):
    """Build Kafka sink partitions for the runtime output topic."""

    def __init__(self, sink: CompiledSink) -> None:
        self._sink = sink

    def build(
        self,
        step_id: str,
        worker_index: int,
        worker_count: int,
    ) -> StatelessSinkPartition[Message[StreamPayload]]:
        return _KafkaMessageSinkPartition(self._sink)


def build_runtime_source(source: CompiledSource) -> _KafkaPollingSource:
    """Build the runtime source for one compiled Kafka input."""
    return _KafkaPollingSource(source)


def build_runtime_sink(sink: CompiledSink) -> _KafkaMessageSink:
    """Build the runtime sink for one compiled Kafka output."""
    return _KafkaMessageSink(sink)


def build_runtime_error_sinks(
    error_routes: dict[ErrorKind, CompiledSink],
) -> dict[ErrorKind, _KafkaMessageSink]:
    """Build runtime sinks for explicit error routes."""
    return {kind: build_runtime_sink(sink) for kind, sink in error_routes.items()}


def build_runtime_terminal_sinks(
    terminal_sinks: dict[tuple[int, ...], CompiledSink],
) -> dict[tuple[int, ...], _KafkaMessageSink]:
    """Build runtime sinks for terminal branch outputs."""
    return {path: build_runtime_sink(sink) for path, sink in terminal_sinks.items()}


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
