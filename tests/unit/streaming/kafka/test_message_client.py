"""Tests for message-level Kafka clients (KafkaMessageProducer, KafkaMessageConsumer)."""

from __future__ import annotations

import pytest
from prometheus_client import CollectorRegistry, generate_latest

from loom.prometheus import KafkaPrometheusMetrics
from loom.streaming.kafka import (
    KafkaDeserializationError,
    KafkaMessageConsumer,
    KafkaMessageProducer,
    KafkaRecord,
    MessageDescriptor,
    MessageEnvelope,
    MsgspecCodec,
)
from tests.unit.streaming.kafka.cases import OrderCreated


class _FakeRawProducer:
    """In-memory raw producer that captures sent records."""

    def __init__(self) -> None:
        self.sent: list[KafkaRecord[bytes]] = []
        self.flushed = False
        self.closed = False
        self.close_error: Exception | None = None

    def send(self, record: KafkaRecord[bytes]) -> None:
        self.sent.append(record)

    def flush(self, timeout_ms: int | None = None) -> None:
        self.flushed = True

    def close(self) -> None:
        if self.close_error is not None:
            raise self.close_error
        self.closed = True


class _FakeRawConsumer:
    """In-memory raw consumer that returns pre-configured records."""

    def __init__(self, records: list[KafkaRecord[bytes] | None]) -> None:
        self._records = list(records)
        self.closed = False
        self.commit_calls: list[bool] = []
        self.close_error: Exception | None = None

    def poll(self, timeout_ms: int) -> KafkaRecord[bytes] | None:
        del timeout_ms
        if self._records:
            return self._records.pop(0)
        return None

    def commit(self, *, asynchronous: bool = False) -> None:
        self.commit_calls.append(asynchronous)

    def close(self) -> None:
        if self.close_error is not None:
            raise self.close_error
        self.closed = True


class _OrderKeyResolver:
    def resolve(self, record: KafkaRecord[MessageEnvelope[OrderCreated]]) -> bytes:
        """Resolve the key from the typed message payload."""

        return record.value.payload.order_id.encode("utf-8")


def test_message_producer_encodes_and_delegates_to_raw(
    order_created_payload: OrderCreated,
    order_created_descriptor_v1: MessageDescriptor,
) -> None:
    raw = _FakeRawProducer()
    codec = MsgspecCodec[OrderCreated]()
    producer = KafkaMessageProducer(raw=raw, codec=codec)

    producer.send(
        topic="orders",
        key="tenant-a",
        payload=order_created_payload,
        descriptor=order_created_descriptor_v1,
        correlation_id="corr-1",
        causation_id="cause-1",
        trace_id="trace-1",
        produced_at_ms=99,
        headers={"h": b"1"},
    )

    assert len(raw.sent) == 1
    record = raw.sent[0]
    assert record.topic == "orders"
    assert record.key == "tenant-a"
    assert record.headers == {"h": b"1"}
    assert record.timestamp_ms == 99
    assert isinstance(record.value, bytes)

    decoded = codec.decode(record.value, OrderCreated)
    assert decoded.payload == order_created_payload
    assert decoded.meta.trace_id == "trace-1"
    assert decoded.meta.correlation_id == "corr-1"
    assert decoded.meta.causation_id == "cause-1"
    assert decoded.meta.produced_at_ms == 99
    assert decoded.meta.descriptor.message_type == "order.created"


def test_message_producer_resolves_key_when_explicit_key_is_absent(
    order_created_payload: OrderCreated,
    order_created_descriptor_v1: MessageDescriptor,
) -> None:
    raw = _FakeRawProducer()
    codec = MsgspecCodec[OrderCreated]()
    producer = KafkaMessageProducer(raw=raw, codec=codec, key_resolver=_OrderKeyResolver())

    producer.send(
        topic="orders",
        payload=order_created_payload,
        descriptor=order_created_descriptor_v1,
    )

    assert len(raw.sent) == 1
    record = raw.sent[0]
    assert record.key == b"o-1"


def test_message_producer_keeps_explicit_key_over_resolved_key(
    order_created_payload: OrderCreated,
    order_created_descriptor_v1: MessageDescriptor,
) -> None:
    raw = _FakeRawProducer()
    codec = MsgspecCodec[OrderCreated]()
    producer = KafkaMessageProducer(raw=raw, codec=codec, key_resolver=_OrderKeyResolver())

    producer.send(
        topic="orders",
        key="tenant-a",
        payload=order_created_payload,
        descriptor=order_created_descriptor_v1,
    )

    assert len(raw.sent) == 1
    record = raw.sent[0]
    assert record.key == "tenant-a"


def test_message_producer_can_disable_record_timestamp(
    order_created_payload: OrderCreated,
    order_created_descriptor_v1: MessageDescriptor,
) -> None:
    raw = _FakeRawProducer()
    codec = MsgspecCodec[OrderCreated]()
    producer = KafkaMessageProducer(raw=raw, codec=codec, use_message_timestamp=False)

    producer.send(
        topic="orders",
        payload=order_created_payload,
        descriptor=order_created_descriptor_v1,
        produced_at_ms=99,
    )

    assert len(raw.sent) == 1
    record = raw.sent[0]
    assert record.timestamp_ms is None


def test_message_producer_flush_and_close_delegate() -> None:
    raw = _FakeRawProducer()
    codec = MsgspecCodec[OrderCreated]()
    producer = KafkaMessageProducer(raw=raw, codec=codec)

    producer.flush(500)
    assert raw.flushed is True

    producer.close()
    assert raw.closed is True


def test_message_consumer_decodes_envelope(
    order_created_payload: OrderCreated,
    order_created_envelope_with_metadata: MessageEnvelope[OrderCreated],
) -> None:
    codec = MsgspecCodec[OrderCreated]()
    encoded = codec.encode(order_created_envelope_with_metadata)
    raw = _FakeRawConsumer(
        [
            KafkaRecord(
                topic="orders",
                key=b"tenant-a",
                value=encoded,
                headers={"x": b"1"},
                partition=2,
                offset=9,
                timestamp_ms=123,
            ),
        ]
    )
    consumer = KafkaMessageConsumer(
        raw=raw,
        codec=codec,
        payload_type=OrderCreated,
    )

    record = consumer.poll(500)

    assert record is not None
    assert record.value.payload == order_created_payload
    assert record.value.meta.trace_id == "trace-1"
    assert record.value.meta.produced_at_ms == 1234
    assert record.topic == "orders"
    assert record.key == b"tenant-a"
    assert record.partition == 2
    assert record.offset == 9


def test_message_consumer_returns_none_when_no_record() -> None:
    codec = MsgspecCodec[OrderCreated]()
    raw = _FakeRawConsumer([None])
    consumer = KafkaMessageConsumer(raw=raw, codec=codec, payload_type=OrderCreated)

    assert consumer.poll(100) is None


def test_message_consumer_raises_deserialization_error() -> None:
    codec = MsgspecCodec[OrderCreated]()
    raw = _FakeRawConsumer(
        [
            KafkaRecord(topic="orders", key=None, value=b"bad-bytes"),
        ]
    )
    consumer = KafkaMessageConsumer(raw=raw, codec=codec, payload_type=OrderCreated)

    with pytest.raises(KafkaDeserializationError):
        consumer.poll(100)


def test_message_consumer_close_delegates() -> None:
    codec = MsgspecCodec[OrderCreated]()
    raw = _FakeRawConsumer([])
    consumer = KafkaMessageConsumer(raw=raw, codec=codec, payload_type=OrderCreated)

    consumer.close()
    assert raw.closed is True


def test_message_consumer_commit_delegates() -> None:
    codec = MsgspecCodec[OrderCreated]()
    raw = _FakeRawConsumer([])
    consumer = KafkaMessageConsumer(raw=raw, codec=codec, payload_type=OrderCreated)

    consumer.commit(asynchronous=True)

    assert raw.commit_calls == [True]


def test_message_producer_emits_encode_metrics(
    kafka_registry: CollectorRegistry,
    kafka_metrics: KafkaPrometheusMetrics,
    order_created_payload: OrderCreated,
    order_created_descriptor_v1: MessageDescriptor,
) -> None:
    raw = _FakeRawProducer()
    codec = MsgspecCodec[OrderCreated]()
    producer = KafkaMessageProducer(raw=raw, codec=codec, observer=kafka_metrics)

    producer.send(
        topic="orders",
        payload=order_created_payload,
        descriptor=order_created_descriptor_v1,
    )

    text = generate_latest(kafka_registry).decode()
    assert "loom_streaming_kafka_encode_duration_seconds" in text


def test_message_consumer_emits_decode_metrics(
    kafka_registry: CollectorRegistry,
    kafka_metrics: KafkaPrometheusMetrics,
    order_created_envelope: MessageEnvelope[OrderCreated],
) -> None:
    codec = MsgspecCodec[OrderCreated]()
    encoded = codec.encode(order_created_envelope)
    raw = _FakeRawConsumer(
        [
            KafkaRecord(topic="orders", key=None, value=encoded),
        ]
    )
    consumer = KafkaMessageConsumer(
        raw=raw,
        codec=codec,
        payload_type=OrderCreated,
        observer=kafka_metrics,
    )

    consumer.poll(100)

    text = generate_latest(kafka_registry).decode()
    assert "loom_streaming_kafka_decode_duration_seconds" in text


def test_message_consumer_emits_error_metric_on_decode_failure(
    kafka_registry: CollectorRegistry,
    kafka_metrics: KafkaPrometheusMetrics,
) -> None:
    codec = MsgspecCodec[OrderCreated]()
    raw = _FakeRawConsumer(
        [
            KafkaRecord(topic="orders", key=None, value=b"bad"),
        ]
    )
    consumer = KafkaMessageConsumer(
        raw=raw,
        codec=codec,
        payload_type=OrderCreated,
        observer=kafka_metrics,
    )

    with pytest.raises(KafkaDeserializationError):
        consumer.poll(100)

    text = generate_latest(kafka_registry).decode()
    assert "loom_streaming_kafka_consumed_total" in text


def test_message_producer_context_manager_closes_raw_on_exit() -> None:
    raw = _FakeRawProducer()
    codec = MsgspecCodec[OrderCreated]()

    with KafkaMessageProducer(raw=raw, codec=codec) as producer:
        producer.send(
            topic="orders",
            payload=OrderCreated(order_id="o-1", amount=1),
            descriptor=MessageDescriptor(message_type="order.created", message_version=1),
        )

    assert raw.closed is True


def test_message_producer_context_manager_does_not_mask_body_exception() -> None:
    raw = _FakeRawProducer()
    raw.close_error = RuntimeError("close-boom")
    codec = MsgspecCodec[OrderCreated]()

    with (
        pytest.raises(ValueError, match="body-boom"),
        KafkaMessageProducer(raw=raw, codec=codec),
    ):
        raise ValueError("body-boom")


def test_message_consumer_context_manager_closes_raw_on_exit() -> None:
    codec = MsgspecCodec[OrderCreated]()
    raw = _FakeRawConsumer([])

    with KafkaMessageConsumer(raw=raw, codec=codec, payload_type=OrderCreated) as consumer:
        consumer.poll(100)

    assert raw.closed is True


def test_message_consumer_context_manager_does_not_mask_body_exception() -> None:
    codec = MsgspecCodec[OrderCreated]()
    raw = _FakeRawConsumer([])
    raw.close_error = RuntimeError("close-boom")

    with (
        pytest.raises(ValueError, match="body-boom"),
        KafkaMessageConsumer(raw=raw, codec=codec, payload_type=OrderCreated),
    ):
        raise ValueError("body-boom")
