"""Message-level Kafka transport protocols."""

from __future__ import annotations

from typing import Protocol, TypeVar

from confluent_kafka import TopicPartition

from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.streaming.kafka._message import MessageDescriptor, MessageEnvelope
from loom.streaming.kafka._record import KafkaRecord

PayloadContraT = TypeVar(
    "PayloadContraT",
    bound=LoomStruct | LoomFrozenStruct,
    contravariant=True,
)
PayloadT = TypeVar("PayloadT", bound=LoomStruct | LoomFrozenStruct)


class MessageProducer(Protocol[PayloadContraT]):
    """Typed message producer contract.

    Builds a standard envelope, encodes via codec, and delegates
    to a raw Kafka producer.
    """

    def send(
        self,
        *,
        topic: str,
        payload: PayloadContraT,
        descriptor: MessageDescriptor,
        key: bytes | str | None = None,
        headers: dict[str, bytes] | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        trace_id: str | None = None,
        produced_at_ms: int | None = None,
    ) -> None:
        """Build, encode, and produce one standard message envelope.

        Args:
            topic: Target Kafka topic.
            payload: Typed payload body.
            descriptor: Stable message descriptor.
            key: Optional Kafka partition key.
            headers: Optional Kafka headers.
            correlation_id: Optional correlation identifier.
            causation_id: Optional upstream message identifier.
            trace_id: Optional explicit trace identifier.
            produced_at_ms: Optional producer timestamp in epoch milliseconds.
        """

    def flush(self, timeout_ms: int | None = None) -> None:
        """Flush pending records.

        Args:
            timeout_ms: Optional maximum flush wait in milliseconds.
        """

    def close(self) -> None:
        """Flush and close the producer."""


class MessageConsumer(Protocol[PayloadT]):
    """Typed message consumer contract.

    Polls raw bytes from a Kafka consumer and decodes them into
    typed message envelopes via codec.
    """

    def poll(self, timeout_ms: int) -> KafkaRecord[MessageEnvelope[PayloadT]] | None:
        """Read and decode one standard message envelope.

        Args:
            timeout_ms: Maximum poll wait in milliseconds.

        Returns:
            One typed record carrying a decoded message envelope, or
            ``None`` when no record is available.
        """

    def commit(self, *, asynchronous: bool = False) -> None:
        """Commit consumed offsets.

        Args:
            asynchronous: Whether the backend may commit asynchronously.
        """

    def commit_offset(self, partitions: list[TopicPartition]) -> None:
        """Commit explicit offsets.

        Args:
            partitions: Kafka topic-partition offsets to commit.
        """

    def close(self) -> None:
        """Close the consumer and release resources."""
