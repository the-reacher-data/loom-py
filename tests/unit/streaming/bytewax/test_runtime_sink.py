"""Tests for runtime Kafka sink wiring in the Bytewax adapter."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from loom.streaming.bytewax import _runtime_io
from loom.streaming.compiler._plan import CompiledSink
from loom.streaming.core._message import Message
from loom.streaming.kafka._errors import KafkaDeliveryError
from loom.streaming.kafka._record import KafkaRecord
from loom.streaming.nodes._boundary import PartitionGuarantee, PartitionPolicy


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


class TestRuntimeSinkPartitionPolicy:
    def test_uses_partition_policy_when_message_has_no_key(
        self,
        monkeypatch: pytest.MonkeyPatch,
        bytewax_runtime_sink_factory: Callable[
            [PartitionPolicy[Any] | None, str | None],
            CompiledSink,
        ],
        bytewax_order_message_factory: Callable[[str, bytes | str | None], Message[Any]],
    ) -> None:
        fake_raw = _FakeRawProducer()
        monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: fake_raw)

        policy = PartitionPolicy(
            strategy=_OrderPartitionStrategy(),
            guarantee=PartitionGuarantee.ENTITY_STABLE,
            allow_repartition=False,
        )
        sink = _runtime_io._KafkaMessageSinkPartition(bytewax_runtime_sink_factory(policy, None))

        sink.write_batch([bytewax_order_message_factory("123", None)])

        assert len(fake_raw.sent) == 1
        record = fake_raw.sent[0]
        assert isinstance(record, KafkaRecord)
        assert record.topic == "orders.out"
        assert record.key == b"order-123"

    def test_preserves_existing_key_without_repartition(
        self,
        monkeypatch: pytest.MonkeyPatch,
        bytewax_runtime_sink_factory: Callable[
            [PartitionPolicy[Any] | None, str | None],
            CompiledSink,
        ],
        bytewax_order_message_factory: Callable[[str, bytes | str | None], Message[Any]],
    ) -> None:
        fake_raw = _FakeRawProducer()
        monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: fake_raw)

        policy = PartitionPolicy(
            strategy=_OrderPartitionStrategy(),
            guarantee=PartitionGuarantee.BEST_EFFORT,
            allow_repartition=False,
        )
        sink = _runtime_io._KafkaMessageSinkPartition(bytewax_runtime_sink_factory(policy, None))

        sink.write_batch([bytewax_order_message_factory("123", b"tenant-a")])

        assert len(fake_raw.sent) == 1
        record = fake_raw.sent[0]
        assert isinstance(record, KafkaRecord)
        assert record.key == b"tenant-a"

    def test_can_override_existing_key_when_repartition_allowed(
        self,
        monkeypatch: pytest.MonkeyPatch,
        bytewax_runtime_sink_factory: Callable[
            [PartitionPolicy[Any] | None, str | None],
            CompiledSink,
        ],
        bytewax_order_message_factory: Callable[[str, bytes | str | None], Message[Any]],
    ) -> None:
        fake_raw = _FakeRawProducer()
        monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: fake_raw)

        policy = PartitionPolicy(
            strategy=_OrderPartitionStrategy(),
            guarantee=PartitionGuarantee.BEST_EFFORT,
            allow_repartition=True,
        )
        sink = _runtime_io._KafkaMessageSinkPartition(bytewax_runtime_sink_factory(policy, None))

        sink.write_batch([bytewax_order_message_factory("123", b"tenant-a")])

        assert len(fake_raw.sent) == 1
        record = fake_raw.sent[0]
        assert isinstance(record, KafkaRecord)
        assert record.key == b"order-123"


class TestDlqRouting:
    def test_routes_failed_batch_to_dlq_topic(
        self,
        monkeypatch: pytest.MonkeyPatch,
        bytewax_runtime_sink_factory: Callable[
            [PartitionPolicy[Any] | None, str | None],
            CompiledSink,
        ],
        bytewax_order_message_factory: Callable[[str, bytes | str | None], Message[Any]],
    ) -> None:
        fake_raw = _FlushErrorProducer()
        monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: fake_raw)

        sink = _runtime_io._KafkaMessageSinkPartition(
            bytewax_runtime_sink_factory(None, "orders.dlq"),
        )

        sink.write_batch([bytewax_order_message_factory("123", None)])

        topics = [r.topic for r in fake_raw.sent]
        assert "orders.out" in topics
        assert "orders.dlq" in topics
        dlq_record = next(r for r in fake_raw.sent if r.topic == "orders.dlq")
        assert b"broker unavailable" in dlq_record.headers.get("x-dlq-error", b"")

    def test_no_dlq_reraises_delivery_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
        bytewax_runtime_sink_factory: Callable[
            [PartitionPolicy[Any] | None, str | None],
            CompiledSink,
        ],
        bytewax_order_message_factory: Callable[[str, bytes | str | None], Message[Any]],
    ) -> None:
        fake_raw = _FlushErrorProducer()
        monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: fake_raw)

        sink = _runtime_io._KafkaMessageSinkPartition(bytewax_runtime_sink_factory(None, None))

        with pytest.raises(KafkaDeliveryError):
            sink.write_batch([bytewax_order_message_factory("123", None)])
