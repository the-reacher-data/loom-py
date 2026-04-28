"""Tests for runtime Kafka source/sink wiring in the Bytewax adapter."""

from __future__ import annotations

from typing import Any

import pytest

from loom.streaming.bytewax import _runtime_io
from loom.streaming.compiler._plan import CompiledSink, CompiledSource
from loom.streaming.core._message import Message, MessageMeta
from loom.streaming.kafka._config import ConsumerSettings, ProducerSettings
from loom.streaming.kafka._errors import KafkaDeliveryError
from loom.streaming.kafka._record import KafkaRecord
from loom.streaming.nodes._boundary import PartitionGuarantee, PartitionPolicy
from loom.streaming.nodes._shape import StreamShape
from tests.unit.streaming.bytewax.cases import Order


class _FakeRawProducer:
    """In-memory raw producer that captures Kafka records."""

    def __init__(self) -> None:
        self.sent: list[object] = []

    def send(self, record: object) -> None:
        self.sent.append(record)

    def flush(self, timeout_ms: int | None = None) -> None:
        del timeout_ms

    def close(self) -> None:
        return None


class _OrderPartitionStrategy:
    """Partition by order id."""

    def partition_key(self, message: Message[Any]) -> bytes | str | None:
        return f"order-{message.payload.order_id}".encode()


def _compiled_sink(
    *,
    partitioning: PartitionPolicy[Any] | None,
) -> CompiledSink:
    return CompiledSink(
        settings=ProducerSettings(
            brokers=("localhost:9092",),
            client_id="test-producer",
            topic="orders.out",
        ),
        topic="orders.out",
        partition_policy=partitioning,
    )


def _message(*, key: bytes | str | None = None) -> Message[Order]:
    return Message(
        payload=Order(order_id="123"),
        meta=MessageMeta(
            message_id="msg-1",
            topic="orders.in",
            key=key,
        ),
    )


class TestRuntimeSinkPartitionPolicy:
    def test_uses_partition_policy_when_message_has_no_key(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake_raw = _FakeRawProducer()

        monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: fake_raw)

        policy = PartitionPolicy(
            strategy=_OrderPartitionStrategy(),
            guarantee=PartitionGuarantee.ENTITY_STABLE,
            allow_repartition=False,
        )
        sink = _runtime_io._KafkaMessageSinkPartition(_compiled_sink(partitioning=policy))

        sink.write_batch([_message()])

        assert len(fake_raw.sent) == 1
        record = fake_raw.sent[0]
        assert isinstance(record, KafkaRecord)
        assert record.topic == "orders.out"
        assert record.key == b"order-123"

    def test_preserves_existing_key_without_repartition(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake_raw = _FakeRawProducer()

        monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: fake_raw)

        policy = PartitionPolicy(
            strategy=_OrderPartitionStrategy(),
            guarantee=PartitionGuarantee.BEST_EFFORT,
            allow_repartition=False,
        )
        sink = _runtime_io._KafkaMessageSinkPartition(_compiled_sink(partitioning=policy))

        sink.write_batch([_message(key=b"tenant-a")])

        assert len(fake_raw.sent) == 1
        record = fake_raw.sent[0]
        assert isinstance(record, KafkaRecord)
        assert record.key == b"tenant-a"

    def test_can_override_existing_key_when_repartition_allowed(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake_raw = _FakeRawProducer()

        monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: fake_raw)

        policy = PartitionPolicy(
            strategy=_OrderPartitionStrategy(),
            guarantee=PartitionGuarantee.BEST_EFFORT,
            allow_repartition=True,
        )
        sink = _runtime_io._KafkaMessageSinkPartition(_compiled_sink(partitioning=policy))

        sink.write_batch([_message(key=b"tenant-a")])

        assert len(fake_raw.sent) == 1
        record = fake_raw.sent[0]
        assert isinstance(record, KafkaRecord)
        assert record.key == b"order-123"


class _FlushErrorProducer:
    """Raw producer that always raises KafkaDeliveryError on flush."""

    def __init__(self) -> None:
        self.sent: list[KafkaRecord[bytes]] = []

    def send(self, record: KafkaRecord[bytes]) -> None:
        self.sent.append(record)

    def flush(self, timeout_ms: int | None = None) -> None:
        raise KafkaDeliveryError("broker unavailable")

    def close(self) -> None:
        return None


def _compiled_sink_with_dlq(dlq_topic: str | None) -> CompiledSink:
    return CompiledSink(
        settings=ProducerSettings(
            brokers=("localhost:9092",),
            client_id="test-producer",
            topic="orders.out",
        ),
        topic="orders.out",
        partition_policy=None,
        dlq_topic=dlq_topic,
    )


class TestDlqRouting:
    def test_routes_failed_batch_to_dlq_topic(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake_raw = _FlushErrorProducer()

        monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: fake_raw)

        sink = _runtime_io._KafkaMessageSinkPartition(
            _compiled_sink_with_dlq(dlq_topic="orders.dlq")
        )

        sink.write_batch([_message()])

        topics = [r.topic for r in fake_raw.sent]
        assert "orders.out" in topics
        assert "orders.dlq" in topics
        dlq_record = next(r for r in fake_raw.sent if r.topic == "orders.dlq")
        assert b"broker unavailable" in dlq_record.headers.get("x-dlq-error", b"")

    def test_no_dlq_reraises_delivery_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake_raw = _FlushErrorProducer()

        monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: fake_raw)

        sink = _runtime_io._KafkaMessageSinkPartition(_compiled_sink_with_dlq(dlq_topic=None))

        with pytest.raises(KafkaDeliveryError):
            sink.write_batch([_message()])


# ---------------------------------------------------------------------------
# KafkaPollingSource poll_timeout_ms
# ---------------------------------------------------------------------------


def _compiled_source(poll_timeout_ms: int = 100) -> CompiledSource:
    return CompiledSource(
        settings=ConsumerSettings(
            brokers=("localhost:9092",),
            group_id="test",
            topics=("orders.in",),
            poll_timeout_ms=poll_timeout_ms,
        ),
        topics=("orders.in",),
        payload_type=Order,
        shape=StreamShape.RECORD,
        decode_strategy="record",
    )


class TestKafkaPollingSource:
    def test_uses_configured_poll_timeout(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """_KafkaPollingSource must forward poll_timeout_ms to the consumer."""
        polled_with: list[int] = []

        class _FakeConsumer:
            def __init__(self, settings: ConsumerSettings) -> None:
                pass

            def poll(self, timeout_ms: int) -> None:
                polled_with.append(timeout_ms)
                return None

            def close(self) -> None:
                pass

        monkeypatch.setattr(_runtime_io, "KafkaConsumerClient", _FakeConsumer)

        source = _runtime_io._KafkaPollingSource(_compiled_source(poll_timeout_ms=250))

        with pytest.raises(_runtime_io._KafkaPollingSource.Retry):
            source.next_item()

        assert polled_with == [250]


class TestConsumerSettings:
    def test_poll_timeout_ms_defaults_to_100(self) -> None:
        settings = ConsumerSettings(
            brokers=("localhost:9092",),
            group_id="test",
            topics=("orders.in",),
        )

        assert settings.poll_timeout_ms == 100
