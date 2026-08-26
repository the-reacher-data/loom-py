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
from loom.streaming.kafka.client._retry import (
    DEFAULT_COORDINATOR_RETRY,
    CoordinatorRetryPolicy,
    with_coordinator_retry,
)


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
        retry_policy: Backoff schedule for transient group-coordinator errors
            on offset fetch and commit.
    """

    def __init__(
        self,
        settings: ConsumerSettings,
        obs: ObservabilityRuntime | None = None,
        *,
        retry_policy: CoordinatorRetryPolicy = DEFAULT_COORDINATOR_RETRY,
        _subscribe: bool = True,
    ) -> None:
        self._consumer = _Consumer(settings.to_confluent_config())
        if _subscribe:
            self._consumer.subscribe(list(settings.topics))
        self._obs = obs
        self._retry_policy = retry_policy

    @classmethod
    def unassigned(
        cls,
        settings: ConsumerSettings,
        observability: ObservabilityRuntime | None = None,
        *,
        retry_policy: CoordinatorRetryPolicy = DEFAULT_COORDINATOR_RETRY,
    ) -> KafkaConsumerClient:
        """Build a consumer with neither subscription nor assignment.

        Used to query the group coordinator (:meth:`committed_offset`) before
        deciding the start offset, then pin the partition with
        :meth:`assign_partition`.

        Args:
            settings: Typed consumer settings.
            observability: Optional observability runtime.
            retry_policy: Backoff schedule for transient group-coordinator
                errors.

        Returns:
            Consumer client not yet attached to any partition.
        """
        return cls(settings, observability, retry_policy=retry_policy, _subscribe=False)

    def assign_partition(self, topic: str, partition: int, offset: int) -> None:
        """Pin this consumer to exactly one topic partition via ``assign``.

        Args:
            topic: Physical topic name.
            partition: Kafka partition index.
            offset: Start offset (a concrete offset or a confluent sentinel
                such as ``OFFSET_BEGINNING``/``OFFSET_END``).
        """
        self._consumer.assign([TopicPartition(topic, partition, offset)])

    def committed_offset(self, topic: str, partition: int, *, timeout_ms: int) -> int | None:
        """Read the consumer group's committed offset for one partition.

        Works without group membership (plain ``OffsetFetch`` to the group
        coordinator), so it is safe on assign-mode and unassigned consumers.

        Args:
            topic: Physical topic name.
            partition: Kafka partition index.
            timeout_ms: Explicit coordinator timeout — a coordinator that does
                not answer is a hard error, never a silent fallback.

        Returns:
            The committed offset, or ``None`` when the group has no valid
            committed offset for the partition.

        Raises:
            KafkaCommitError: If the offset fetch fails or times out.
        """
        try:
            results = with_coordinator_retry(
                lambda: self._consumer.committed(
                    [TopicPartition(topic, partition)], timeout=timeout_ms / 1000
                ),
                policy=self._retry_policy,
            )
        except Exception as exc:
            raise KafkaCommitError(str(exc)) from exc
        if not results:
            return None
        result = results[0]
        if result.error is not None:
            raise KafkaCommitError(str(result.error))
        offset = result.offset
        if offset is None or offset < 0:
            return None
        return int(offset)

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
        partition.  Compacted-topic tombstones (records with a ``None``
        value) are skipped: they carry no payload to decode, and the
        gap-tolerant commit watermark treats unregistered offsets as gaps,
        so skipping never freezes commits.

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
        records: list[KafkaRecord[bytes]] = []
        for message in messages:
            error = message.error()
            if error is not None:
                raise KafkaPollError(str(error))
            if message.value() is None:
                continue
            records.append(_to_record(message))
        return records

    def commit(self, *, asynchronous: bool = False) -> None:
        """Commit consumed offsets.

        Args:
            asynchronous: Whether the backend may commit asynchronously.

        Raises:
            KafkaCommitError: If the backend commit fails.
        """
        try:
            commit = cast(_CommitMethod, self._consumer.commit)
            with_coordinator_retry(
                lambda: commit(asynchronous=asynchronous),
                policy=self._retry_policy,
            )
        except Exception as exc:
            raise KafkaCommitError(str(exc)) from exc

    def commit_offset(
        self,
        partitions: list[TopicPartition],
        *,
        asynchronous: bool = False,
    ) -> None:
        """Commit explicit Kafka offsets.

        Args:
            partitions: Kafka topic-partition offsets to commit.
            asynchronous: When ``True``, librdkafka coalesces the commit in
                the background; failures surface through the consumer's
                logger/``on_commit`` callback rather than as an exception.

        Raises:
            KafkaCommitError: If the backend commit fails (synchronous mode).
        """
        try:
            commit = cast(_CommitMethod, self._consumer.commit)
            with_coordinator_retry(
                lambda: commit(offsets=partitions, asynchronous=asynchronous),
                policy=self._retry_policy,
            )
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
