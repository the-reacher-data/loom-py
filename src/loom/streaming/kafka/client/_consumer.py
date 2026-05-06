"""Raw Kafka consumer backed by confluent-kafka."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Literal, Protocol, cast

from confluent_kafka import Consumer as _Consumer
from confluent_kafka import Message as _RawMessage
from confluent_kafka import TopicPartition

from loom.core.observability.event import LifecycleEvent, Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.streaming.kafka._config import ConsumerSettings
from loom.streaming.kafka._errors import KafkaCommitError, KafkaPollError
from loom.streaming.kafka._record import KafkaRecord


class _CommitMethod(Protocol):
    def __call__(
        self,
        *,
        asynchronous: bool = ...,
        offsets: list[TopicPartition] | None = ...,
    ) -> object: ...


class KafkaConsumerClient:
    """Confluent-backed raw Kafka consumer.

    Returns ``KafkaRecord[bytes]`` from Kafka. No deserialization —
    values are raw bytes as received from the broker.

    Args:
        settings: Typed consumer settings.
        observer: Optional observability observer.
    """

    def __init__(
        self,
        settings: ConsumerSettings,
        obs: ObservabilityRuntime | None = None,
    ) -> None:
        self._consumer = _Consumer(settings.to_confluent_config())
        self._consumer.subscribe(list(settings.topics))
        self._obs = obs

    def poll(self, timeout_ms: int) -> KafkaRecord[bytes] | None:
        """Read one raw byte record from Kafka.

        Args:
            timeout_ms: Maximum poll wait in milliseconds.

        Returns:
            One raw Kafka record or ``None`` when no record is available.

        Raises:
            KafkaPollError: If the backend poll fails or returns a broker
                error.
        """
        try:
            message = self._consumer.poll(timeout_ms / 1000)
        except Exception as exc:
            raise KafkaPollError(str(exc)) from exc
        if message is None:
            return None
        if message.error() is not None:
            raise KafkaPollError(str(message.error()))
        record = _to_record(message)
        if self._obs is not None:
            self._obs.emit(
                LifecycleEvent.end(
                    scope=Scope.TRANSPORT,
                    name="kafka_consume",
                    meta={"topic": record.topic},
                )
            )
        return record

    def commit(self, *, asynchronous: bool = False) -> None:
        """Commit consumed offsets.

        Args:
            asynchronous: Whether the backend may commit asynchronously.

        Raises:
            KafkaCommitError: If the backend commit fails.
        """
        try:
            commit = cast(_CommitMethod, self._consumer.commit)
            commit(asynchronous=asynchronous)
        except Exception as exc:
            raise KafkaCommitError(str(exc)) from exc

    def commit_offset(self, partitions: list[TopicPartition]) -> None:
        """Commit explicit Kafka offsets.

        Args:
            partitions: Kafka topic-partition offsets to commit.

        Raises:
            KafkaCommitError: If the backend commit fails.
        """
        try:
            commit = cast(_CommitMethod, self._consumer.commit)
            commit(offsets=partitions, asynchronous=False)
        except Exception as exc:
            raise KafkaCommitError(str(exc)) from exc

    def close(self) -> None:
        """Close the consumer and release resources."""
        self._consumer.close()

    def __enter__(self) -> KafkaConsumerClient:
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


def _to_record(message: _RawMessage) -> KafkaRecord[bytes]:
    value_bytes = message.value()
    if value_bytes is None:
        raise TypeError("KafkaConsumerClient received a record without a value")
    _, timestamp_ms = message.timestamp()
    headers = _normalize_headers(message.headers())
    key = message.key()
    normalized_key: bytes | str | None = None if key is None else key
    topic = message.topic()
    if topic is None:
        raise TypeError("KafkaConsumerClient received a record without a topic")
    return KafkaRecord(
        topic=topic,
        key=normalized_key,
        value=value_bytes,
        headers=headers,
        partition=message.partition(),
        offset=message.offset(),
        timestamp_ms=timestamp_ms if timestamp_ms >= 0 else None,
    )


def _normalize_headers(
    raw_headers: Mapping[str, str | bytes | None] | Iterable[tuple[str, str | bytes | None]] | None,
) -> dict[str, bytes]:
    """Return Kafka headers without tombstone header values."""
    headers: dict[str, bytes] = {}
    if raw_headers is None:
        return headers
    if isinstance(raw_headers, Mapping):
        iterable = cast(Iterable[tuple[str, str | bytes | None]], raw_headers.items())
    else:
        iterable = raw_headers
    for header_key, header_value in iterable:
        if header_value is not None:
            headers[header_key] = (
                header_value if isinstance(header_value, bytes) else header_value.encode("utf-8")
            )
    return headers
