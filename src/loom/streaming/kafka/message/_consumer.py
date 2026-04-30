"""Message-level Kafka consumer with codec and envelope semantics."""

from __future__ import annotations

from time import perf_counter
from typing import TYPE_CHECKING, Generic, Literal, TypeVar

from confluent_kafka import TopicPartition

from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.streaming.kafka._codec import KafkaCodec
from loom.streaming.kafka._errors import KafkaDeserializationError
from loom.streaming.kafka._message import MessageEnvelope
from loom.streaming.kafka._record import KafkaRecord
from loom.streaming.kafka.client._protocol import KafkaConsumer

if TYPE_CHECKING:
    from loom.streaming.observability.observers import KafkaStreamingObserver

PayloadT = TypeVar("PayloadT", bound=LoomStruct | LoomFrozenStruct)


class KafkaMessageConsumer(Generic[PayloadT]):
    """Polls raw bytes from a Kafka consumer and decodes them via codec.

    All dependencies are injected via constructor — the message consumer
    depends on the ``KafkaConsumer`` and ``KafkaCodec`` protocols, not on
    concrete implementations.

    Args:
        raw: Raw Kafka consumer for byte-level transport.
        codec: Codec for decoding bytes to message envelopes.
        payload_type: Expected payload model type for decoding.
        observer: Optional observability observer.
    """

    def __init__(
        self,
        raw: KafkaConsumer,
        codec: KafkaCodec[PayloadT],
        payload_type: type[PayloadT],
        observer: KafkaStreamingObserver | None = None,
    ) -> None:
        self._raw = raw
        self._codec = codec
        self._payload_type = payload_type
        self._observer = observer

    def poll(self, timeout_ms: int) -> KafkaRecord[MessageEnvelope[PayloadT]] | None:
        """Read and decode one standard message envelope from Kafka.

        Args:
            timeout_ms: Maximum poll wait in milliseconds.

        Returns:
            One typed record carrying a decoded message envelope, or
            ``None`` when no record is available.

        Raises:
            KafkaPollError: If the backend poll fails.
            KafkaDeserializationError: If the envelope cannot be decoded.
        """
        record = self._raw.poll(timeout_ms)
        if record is None:
            return None
        started = perf_counter()
        try:
            message = self._codec.decode(record.value, self._payload_type)
        except Exception as exc:
            if self._observer is not None:
                self._observer.on_consumed(record.topic, status="decode_error")
            raise KafkaDeserializationError(str(exc)) from exc
        if self._observer is not None:
            self._observer.observe_decode(
                message.meta.descriptor.content_type.media_type,
                perf_counter() - started,
            )
        return KafkaRecord(
            topic=record.topic,
            key=record.key,
            value=message,
            headers=record.headers,
            partition=record.partition,
            offset=record.offset,
            timestamp_ms=record.timestamp_ms,
        )

    def commit(self, *, asynchronous: bool = False) -> None:
        """Commit consumed offsets through the raw consumer.

        Args:
            asynchronous: Whether the backend may commit asynchronously.
        """
        self._raw.commit(asynchronous=asynchronous)

    def commit_offset(self, partitions: list[TopicPartition]) -> None:
        """Commit explicit offsets through the raw consumer.

        Args:
            partitions: Kafka topic-partition offsets to commit.
        """
        self._raw.commit_offset(partitions)

    def close(self) -> None:
        """Close the consumer and release resources."""
        self._raw.close()

    def __enter__(self) -> KafkaMessageConsumer[PayloadT]:
        """Return self for context-manager usage."""
        return self

    def __exit__(self, *exc: object) -> Literal[False]:
        """Close the consumer on context exit."""
        try:
            self.close()
        except Exception:
            if exc[0] is None:
                raise
        return False
