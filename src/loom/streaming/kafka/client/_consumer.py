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
from loom.streaming.kafka._message import HEADER_CORRELATION_ID, HEADER_TRACE_ID
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
        obs: Optional observability runtime.
    """

    def __init__(
        self,
        settings: ConsumerSettings,
        obs: ObservabilityRuntime | None = None,
        *,
        _subscribe: bool = True,
    ) -> None:
        self._consumer = _Consumer(settings.to_confluent_config())
        if _subscribe:
            self._consumer.subscribe(list(settings.topics))
        self._obs = obs

    @classmethod
    def for_partition(
        cls,
        settings: ConsumerSettings,
        *,
        topic: str,
        partition: int,
        offset: int,
        observability: ObservabilityRuntime | None = None,
    ) -> KafkaConsumerClient:
        """Build a consumer pinned to one partition via Kafka ``assign``.

        The consumer never calls ``subscribe`` and therefore never joins
        group membership: the consumer group acts only as an offset store.

        Args:
            settings: Typed consumer settings.
            topic: Physical topic name.
            partition: Kafka partition index.
            offset: Start offset passed to ``assign``.
            observability: Optional observability runtime.

        Returns:
            Consumer client assigned to exactly one topic partition.
        """
        client = cls(settings, observability, _subscribe=False)
        client._consumer.assign([TopicPartition(topic, partition, offset)])
        return client

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
        record = _checked_record(message)
        if self._obs is not None:
            self._obs.emit(
                LifecycleEvent.end(
                    scope=Scope.TRANSPORT,
                    name="kafka_consume",
                    trace_id=_header_trace_id(record.headers),
                    correlation_id=_header_correlation_id(record.headers),
                    meta={"topic": record.topic},
                )
            )
        return record

    def consume_batch(self, max_records: int) -> list[KafkaRecord[bytes]]:
        """Read up to ``max_records`` raw byte records without blocking.

        Uses a negligible backend timeout, so the call returns whatever the
        consumer already buffered.  Record order is the broker order per
        partition.

        Args:
            max_records: Maximum number of records to return.

        Returns:
            Raw Kafka records; empty when nothing is available.

        Raises:
            KafkaPollError: If the backend consume fails or any message
                carries a broker error.
        """
        try:
            messages = self._consumer.consume(max_records, timeout=0.001)
        except Exception as exc:
            raise KafkaPollError(str(exc)) from exc
        return [_checked_record(message) for message in messages]

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


def _checked_record(message: _RawMessage) -> KafkaRecord[bytes]:
    """Translate one confluent message, raising on broker-reported errors."""
    error = message.error()
    if error is not None:
        raise KafkaPollError(str(error))
    return _to_record(message)


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


def _header_trace_id(headers: dict[str, bytes]) -> str | None:
    raw = headers.get(HEADER_TRACE_ID)
    return raw.decode() if raw is not None else None


def _header_correlation_id(headers: dict[str, bytes]) -> str | None:
    raw = headers.get(HEADER_CORRELATION_ID)
    return raw.decode() if raw is not None else None


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
