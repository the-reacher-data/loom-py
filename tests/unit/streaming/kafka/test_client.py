"""Tests for raw Kafka transport clients (KafkaProducerClient, KafkaConsumerClient)."""

from __future__ import annotations

import pytest
from confluent_kafka import TopicPartition
from prometheus_client import CollectorRegistry, generate_latest

from loom.prometheus import KafkaPrometheusMetrics
from loom.streaming.kafka import (
    ConsumerSettings,
    KafkaCommitError,
    KafkaConsumerClient,
    KafkaDeliveryError,
    KafkaPollError,
    KafkaProducerClient,
    KafkaRecord,
    ProducerSettings,
)
from tests.unit.streaming.kafka.fakes import (
    ConsumerBackendStub,
    FakeDeliveryError,
    FakeKafkaMessage,
    ProducerBackendStub,
    install_raw_consumer_stub,
    install_raw_producer_stub,
)

pytestmark = pytest.mark.kafka


class _FakeError:
    def __str__(self) -> str:
        return "boom"


class TestKafkaProducerClient:
    def test_raw_producer_sends_bytes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        installer = install_raw_producer_stub(monkeypatch)
        producer = KafkaProducerClient(
            ProducerSettings(brokers=("k1:9092",), client_id="p1"),
        )

        producer.send(
            KafkaRecord(
                topic="orders",
                key="tenant-a",
                value=b"raw-payload",
                headers={"h": b"1"},
                timestamp_ms=44,
            )
        )
        producer.flush(250)
        producer.close()

        fake = installer.stub
        assert fake is not None
        assert fake.config["bootstrap.servers"] == "k1:9092"
        assert fake.produced[0]["topic"] == "orders"
        assert fake.produced[0]["key"] == b"tenant-a"
        assert fake.produced[0]["value"] == b"raw-payload"
        assert fake.produced[0]["headers"] == [("h", b"1")]
        assert fake.produced[0]["timestamp"] == 44
        assert fake.poll_calls == [0.0]
        assert fake.flush_calls == [0.25, None]

    def test_raw_producer_delivery_callback_and_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        seen: list[KafkaDeliveryError | None] = []
        installer = install_raw_producer_stub(monkeypatch)
        producer = KafkaProducerClient(
            ProducerSettings(brokers=("k1:9092",)),
            delivery_callback=lambda record, error: seen.append(error),
        )

        producer.send(KafkaRecord(topic="orders", key=b"k", value=b"payload"))
        fake = installer.stub
        assert fake is not None
        callback = fake.produced[0]["on_delivery"]
        assert callback is not None

        callback(FakeDeliveryError(), None)

        with pytest.raises(KafkaDeliveryError, match="delivery-boom"):
            producer.flush()
        assert seen[-1] is not None

    def test_raw_producer_flush_consumes_pending_delivery_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        installer = install_raw_producer_stub(monkeypatch)
        producer = KafkaProducerClient(ProducerSettings(brokers=("k1:9092",)))
        producer.send(KafkaRecord(topic="orders", key=b"k", value=b"payload"))
        fake = installer.stub
        assert fake is not None
        fake.produced[0]["on_delivery"](FakeDeliveryError(), None)

        with pytest.raises(KafkaDeliveryError, match="delivery-boom"):
            producer.flush()
        producer.flush()

    def test_raw_producer_emits_metrics(
        self,
        monkeypatch: pytest.MonkeyPatch,
        kafka_registry: CollectorRegistry,
        kafka_metrics: KafkaPrometheusMetrics,
    ) -> None:
        installer = install_raw_producer_stub(monkeypatch)
        producer = KafkaProducerClient(
            ProducerSettings(brokers=("k1:9092",)),
            observer=kafka_metrics,
        )

        producer.send(KafkaRecord(topic="orders", key=b"k", value=b"payload"))
        fake = installer.stub
        assert fake is not None
        fake.produced[0]["on_delivery"](None, None)
        producer.flush()

        text = generate_latest(kafka_registry).decode()
        assert "loom_streaming_kafka_produced_total" in text

    def test_raw_producer_context_manager_closes_on_exit(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        installer = install_raw_producer_stub(monkeypatch)

        with KafkaProducerClient(ProducerSettings(brokers=("k1:9092",))) as producer:
            producer.send(KafkaRecord(topic="orders", key=b"k", value=b"payload"))

        fake = installer.stub
        assert fake is not None
        assert fake.flush_calls == [None]

    def test_raw_producer_context_manager_does_not_mask_body_exception(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        class _FlushFailingProducer(ProducerBackendStub):
            def flush(self, timeout: float | None = None) -> None:
                super().flush(timeout)
                raise RuntimeError("close-boom")

        monkeypatch.setattr(
            "loom.streaming.kafka.client._producer._Producer",
            lambda config: _FlushFailingProducer(config),
        )

        with (
            pytest.raises(ValueError, match="body-boom"),
            KafkaProducerClient(ProducerSettings(brokers=("k1:9092",))),
        ):
            raise ValueError("body-boom")


class TestKafkaConsumerClient:
    def test_raw_consumer_polls_bytes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        fake = installer.stub
        assert fake is not None
        fake.next_message = FakeKafkaMessage(value=b"raw-bytes")
        consumer = KafkaConsumerClient(
            ConsumerSettings(brokers=("k1:9092",), group_id="g1", topics=("orders",)),
        )

        record = consumer.poll(500)
        consumer.close()

        assert fake.subscribed == ["orders"]
        assert fake.poll_calls == [0.5]
        assert fake.closed is True
        assert record is not None
        assert record.topic == "orders"
        assert record.key == b"tenant-a"
        assert record.value == b"raw-bytes"
        assert record.headers == {"x": b"1"}
        assert record.partition == 2
        assert record.offset == 9
        assert record.timestamp_ms == 123

    def test_raw_consumer_returns_none_when_no_message(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        fake = installer.stub
        assert fake is not None
        consumer = KafkaConsumerClient(
            ConsumerSettings(brokers=("k1:9092",), group_id="g1", topics=("orders",)),
        )
        assert consumer.poll(100) is None

    def test_raw_consumer_raises_on_kafka_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        fake = installer.stub
        assert fake is not None
        fake.next_message = FakeKafkaMessage(error=_FakeError())
        consumer = KafkaConsumerClient(
            ConsumerSettings(brokers=("k1:9092",), group_id="g1", topics=("orders",)),
        )
        with pytest.raises(KafkaPollError, match="boom"):
            consumer.poll(100)

    def test_raw_consumer_commit_delegates(self, monkeypatch: pytest.MonkeyPatch) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        fake = installer.stub
        assert fake is not None
        consumer = KafkaConsumerClient(
            ConsumerSettings(brokers=("k1:9092",), group_id="g1", topics=("orders",)),
        )

        consumer.commit(asynchronous=True)

        assert fake.commit_calls == [True]

    def test_raw_consumer_commit_offset_delegates(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        fake = installer.stub
        assert fake is not None
        consumer = KafkaConsumerClient(
            ConsumerSettings(brokers=("k1:9092",), group_id="g1", topics=("orders",)),
        )

        consumer.commit_offset([TopicPartition("orders", 1, 12)])

        assert len(fake.commit_offset_calls) == 1
        assert fake.commit_offset_calls[0][0].topic == "orders"
        assert fake.commit_offset_calls[0][0].partition == 1
        assert fake.commit_offset_calls[0][0].offset == 12

    def test_raw_consumer_commit_offset_wraps_backend_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        fake = installer.stub
        assert fake is not None
        fake.commit_error = RuntimeError("commit-boom")
        consumer = KafkaConsumerClient(
            ConsumerSettings(brokers=("k1:9092",), group_id="g1", topics=("orders",)),
        )

        with pytest.raises(KafkaCommitError, match="commit-boom"):
            consumer.commit_offset([TopicPartition("orders", 1, 12)])

    def test_raw_consumer_commit_wraps_backend_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        fake = installer.stub
        assert fake is not None
        fake.commit_error = RuntimeError("commit-boom")
        consumer = KafkaConsumerClient(
            ConsumerSettings(brokers=("k1:9092",), group_id="g1", topics=("orders",)),
        )

        with pytest.raises(KafkaCommitError, match="commit-boom"):
            consumer.commit()

    def test_raw_consumer_poll_does_not_wrap_keyboard_interrupt(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        class _InterruptingConsumer(ConsumerBackendStub):
            def poll(self, timeout: float) -> FakeKafkaMessage | None:
                del timeout
                raise KeyboardInterrupt

        fake = _InterruptingConsumer({})
        monkeypatch.setattr("loom.streaming.kafka.client._consumer._Consumer", lambda config: fake)
        consumer = KafkaConsumerClient(
            ConsumerSettings(brokers=("k1:9092",), group_id="g1", topics=("orders",)),
        )

        with pytest.raises(KeyboardInterrupt):
            consumer.poll(100)

    def test_raw_consumer_emits_metrics(
        self,
        monkeypatch: pytest.MonkeyPatch,
        kafka_registry: CollectorRegistry,
        kafka_metrics: KafkaPrometheusMetrics,
    ) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        fake = installer.stub
        assert fake is not None
        fake.next_message = FakeKafkaMessage(value=b"data")
        consumer = KafkaConsumerClient(
            ConsumerSettings(brokers=("k1:9092",), group_id="g1", topics=("orders",)),
            observer=kafka_metrics,
        )

        consumer.poll(100)

        text = generate_latest(kafka_registry).decode()
        assert "loom_streaming_kafka_consumed_total" in text

    def test_raw_consumer_context_manager_closes_on_exit(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        fake = installer.stub
        assert fake is not None

        with KafkaConsumerClient(
            ConsumerSettings(brokers=("k1:9092",), group_id="g1", topics=("orders",)),
        ) as consumer:
            consumer.poll(100)

        assert fake.closed is True

    def test_raw_consumer_context_manager_does_not_mask_body_exception(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        fake = installer.stub
        assert fake is not None
        fake.close_error = RuntimeError("close-boom")

        with (
            pytest.raises(ValueError, match="body-boom"),
            KafkaConsumerClient(
                ConsumerSettings(brokers=("k1:9092",), group_id="g1", topics=("orders",)),
            ),
        ):
            raise ValueError("body-boom")
