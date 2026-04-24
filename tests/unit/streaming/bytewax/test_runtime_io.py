"""Tests for runtime Kafka source/sink wiring in the Bytewax adapter."""

from __future__ import annotations

import pytest

pytest.importorskip("bytewax")

from loom.core.model import LoomStruct
from loom.streaming.bytewax import _runtime_io
from loom.streaming.compiler._plan import CompiledSink
from loom.streaming.core._message import Message, MessageMeta
from loom.streaming.kafka._config import ProducerSettings
from loom.streaming.kafka._record import KafkaRecord
from loom.streaming.nodes._boundary import PartitionGuarantee, PartitionPolicy


class _Order(LoomStruct):
    order_id: str


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

    def partition_key(self, message: Message[_Order]) -> bytes | str | None:
        return f"order-{message.payload.order_id}".encode()


def _compiled_sink(
    *,
    partitioning: PartitionPolicy[_Order] | None,
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


def _message(*, key: bytes | str | None = None) -> Message[_Order]:
    return Message(
        payload=_Order(order_id="123"),
        meta=MessageMeta(
            message_id="msg-1",
            topic="orders.in",
            key=key,
        ),
    )


def test_runtime_sink_uses_partition_policy_when_message_has_no_key(
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


def test_runtime_sink_preserves_existing_key_without_repartition(
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


def test_runtime_sink_can_override_existing_key_when_repartition_allowed(
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
