"""Tests for raw Kafka transport clients (KafkaProducerClient, KafkaConsumerClient)."""

from __future__ import annotations

from typing import Any

import pytest

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


class _FakeDeliveryError:
    def __str__(self) -> str:
        return "delivery-boom"


class _FakeProducer:
    def __init__(self, config: dict[str, str]) -> None:
        self.config = config
        self.produced: list[dict[str, Any]] = []
        self.flush_calls: list[float | None] = []
        self.poll_calls: list[float] = []

    def produce(
        self,
        *,
        topic: str,
        key: bytes | None,
        value: bytes,
        headers: list[tuple[str, bytes]] | None,
        timestamp: int | None = None,
        on_delivery: Any = None,
    ) -> None:
        self.produced.append(
            {
                "topic": topic,
                "key": key,
                "value": value,
                "headers": headers,
                "timestamp": timestamp,
                "on_delivery": on_delivery,
            }
        )

    def poll(self, timeout: float) -> None:
        self.poll_calls.append(timeout)

    def flush(self, timeout: float | None = None) -> None:
        self.flush_calls.append(timeout)


class _FakeError:
    def __str__(self) -> str:
        return "boom"


class _FakeMessage:
    def __init__(
        self,
        *,
        topic: str = "orders",
        key: bytes | None = b"tenant-a",
        value: bytes | None = b"payload",
        headers: list[tuple[str, bytes | None]] | None = None,
        partition: int = 2,
        offset: int = 9,
        timestamp_ms: int = 123,
        error: object | None = None,
    ) -> None:
        self._topic = topic
        self._key = key
        self._value = value
        self._headers = headers if headers is not None else [("x", b"1")]
        self._partition = partition
        self._offset = offset
        self._timestamp_ms = timestamp_ms
        self._error = error

    def error(self) -> object | None:
        return self._error

    def value(self) -> bytes | None:
        return self._value

    def timestamp(self) -> tuple[int, int]:
        return (0, self._timestamp_ms)

    def headers(self) -> list[tuple[str, bytes | None]]:
        return self._headers

    def key(self) -> bytes | None:
        return self._key

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


class _FakeConsumer:
    def __init__(self, config: dict[str, str]) -> None:
        self.config = config
        self.subscribed: list[str] = []
        self.next_message: _FakeMessage | None = None
        self.closed = False
        self.poll_calls: list[float] = []
        self.commit_calls: list[bool] = []
        self.commit_error: Exception | None = None
        self.close_error: Exception | None = None

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed = topics

    def poll(self, timeout: float) -> _FakeMessage | None:
        self.poll_calls.append(timeout)
        return self.next_message

    def commit(self, *, asynchronous: bool = False) -> None:
        if self.commit_error is not None:
            raise self.commit_error
        self.commit_calls.append(asynchronous)

    def close(self) -> None:
        if self.close_error is not None:
            raise self.close_error
        self.closed = True


def test_raw_producer_sends_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    created: dict[str, _FakeProducer] = {}

    def _build(config: dict[str, str]) -> _FakeProducer:
        fake = _FakeProducer(config)
        created["p"] = fake
        return fake

    monkeypatch.setattr(
        "loom.streaming.kafka.client._producer._Producer",
        _build,
    )
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

    fake = created["p"]
    assert fake.config["bootstrap.servers"] == "k1:9092"
    assert fake.produced[0]["topic"] == "orders"
    assert fake.produced[0]["key"] == b"tenant-a"
    assert fake.produced[0]["value"] == b"raw-payload"
    assert fake.produced[0]["headers"] == [("h", b"1")]
    assert fake.produced[0]["timestamp"] == 44
    assert fake.poll_calls == [0.0]
    assert fake.flush_calls == [0.25, None]


def test_raw_consumer_polls_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeConsumer({})
    fake.next_message = _FakeMessage(value=b"raw-bytes")
    monkeypatch.setattr(
        "loom.streaming.kafka.client._consumer._Consumer",
        lambda config: fake,
    )
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


def test_raw_consumer_returns_none_when_no_message(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeConsumer({})
    monkeypatch.setattr(
        "loom.streaming.kafka.client._consumer._Consumer",
        lambda config: fake,
    )
    consumer = KafkaConsumerClient(
        ConsumerSettings(brokers=("k1:9092",), group_id="g1", topics=("orders",)),
    )
    assert consumer.poll(100) is None


def test_raw_consumer_raises_on_kafka_error(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeConsumer({})
    fake.next_message = _FakeMessage(error=_FakeError())
    monkeypatch.setattr(
        "loom.streaming.kafka.client._consumer._Consumer",
        lambda config: fake,
    )
    consumer = KafkaConsumerClient(
        ConsumerSettings(brokers=("k1:9092",), group_id="g1", topics=("orders",)),
    )
    with pytest.raises(KafkaPollError, match="boom"):
        consumer.poll(100)


def test_raw_consumer_commit_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeConsumer({})
    monkeypatch.setattr(
        "loom.streaming.kafka.client._consumer._Consumer",
        lambda config: fake,
    )
    consumer = KafkaConsumerClient(
        ConsumerSettings(brokers=("k1:9092",), group_id="g1", topics=("orders",)),
    )

    consumer.commit(asynchronous=True)

    assert fake.commit_calls == [True]


def test_raw_consumer_commit_wraps_backend_error(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeConsumer({})
    fake.commit_error = RuntimeError("commit-boom")
    monkeypatch.setattr(
        "loom.streaming.kafka.client._consumer._Consumer",
        lambda config: fake,
    )
    consumer = KafkaConsumerClient(
        ConsumerSettings(brokers=("k1:9092",), group_id="g1", topics=("orders",)),
    )

    with pytest.raises(KafkaCommitError, match="commit-boom"):
        consumer.commit()


def test_raw_consumer_poll_does_not_wrap_keyboard_interrupt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _InterruptingConsumer(_FakeConsumer):
        def poll(self, timeout: float) -> _FakeMessage | None:
            del timeout
            raise KeyboardInterrupt

    fake = _InterruptingConsumer({})
    monkeypatch.setattr(
        "loom.streaming.kafka.client._consumer._Consumer",
        lambda config: fake,
    )
    consumer = KafkaConsumerClient(
        ConsumerSettings(brokers=("k1:9092",), group_id="g1", topics=("orders",)),
    )

    with pytest.raises(KeyboardInterrupt):
        consumer.poll(100)


def test_raw_producer_delivery_callback_and_error(monkeypatch: pytest.MonkeyPatch) -> None:
    created: dict[str, _FakeProducer] = {}
    seen: list[KafkaDeliveryError | None] = []

    def _build(config: dict[str, str]) -> _FakeProducer:
        fake = _FakeProducer(config)
        created["p"] = fake
        return fake

    monkeypatch.setattr(
        "loom.streaming.kafka.client._producer._Producer",
        _build,
    )
    producer = KafkaProducerClient(
        ProducerSettings(brokers=("k1:9092",)),
        delivery_callback=lambda record, error: seen.append(error),
    )

    producer.send(KafkaRecord(topic="orders", key=b"k", value=b"payload"))
    callback = created["p"].produced[0]["on_delivery"]
    assert callback is not None

    callback(_FakeDeliveryError(), None)

    with pytest.raises(KafkaDeliveryError, match="delivery-boom"):
        producer.flush()
    assert seen[-1] is not None


def test_raw_producer_flush_consumes_pending_delivery_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: dict[str, _FakeProducer] = {}

    def _build(config: dict[str, str]) -> _FakeProducer:
        fake = _FakeProducer(config)
        created["p"] = fake
        return fake

    monkeypatch.setattr(
        "loom.streaming.kafka.client._producer._Producer",
        _build,
    )
    producer = KafkaProducerClient(ProducerSettings(brokers=("k1:9092",)))
    producer.send(KafkaRecord(topic="orders", key=b"k", value=b"payload"))
    created["p"].produced[0]["on_delivery"](_FakeDeliveryError(), None)

    with pytest.raises(KafkaDeliveryError, match="delivery-boom"):
        producer.flush()
    producer.flush()


def test_raw_producer_emits_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    from prometheus_client import CollectorRegistry, generate_latest

    created: dict[str, _FakeProducer] = {}

    def _build(config: dict[str, str]) -> _FakeProducer:
        fake = _FakeProducer(config)
        created["p"] = fake
        return fake

    registry = CollectorRegistry()
    metrics = KafkaPrometheusMetrics(registry=registry)
    monkeypatch.setattr(
        "loom.streaming.kafka.client._producer._Producer",
        _build,
    )
    producer = KafkaProducerClient(
        ProducerSettings(brokers=("k1:9092",)),
        observer=metrics,
    )

    producer.send(KafkaRecord(topic="orders", key=b"k", value=b"payload"))
    created["p"].produced[0]["on_delivery"](None, None)
    producer.flush()

    text = generate_latest(registry).decode()
    assert "loom_streaming_kafka_produced_total" in text


def test_raw_consumer_emits_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    from prometheus_client import CollectorRegistry, generate_latest

    registry = CollectorRegistry()
    metrics = KafkaPrometheusMetrics(registry=registry)
    fake = _FakeConsumer({})
    fake.next_message = _FakeMessage(value=b"data")
    monkeypatch.setattr(
        "loom.streaming.kafka.client._consumer._Consumer",
        lambda config: fake,
    )
    consumer = KafkaConsumerClient(
        ConsumerSettings(brokers=("k1:9092",), group_id="g1", topics=("orders",)),
        observer=metrics,
    )

    consumer.poll(100)

    text = generate_latest(registry).decode()
    assert "loom_streaming_kafka_consumed_total" in text


def test_raw_producer_context_manager_closes_on_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    created: dict[str, _FakeProducer] = {}

    def _build(config: dict[str, str]) -> _FakeProducer:
        fake = _FakeProducer(config)
        created["p"] = fake
        return fake

    monkeypatch.setattr(
        "loom.streaming.kafka.client._producer._Producer",
        _build,
    )

    with KafkaProducerClient(ProducerSettings(brokers=("k1:9092",))) as producer:
        producer.send(KafkaRecord(topic="orders", key=b"k", value=b"payload"))

    assert created["p"].flush_calls == [None]


def test_raw_producer_context_manager_does_not_mask_body_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FlushFailingProducer(_FakeProducer):
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


def test_raw_consumer_context_manager_closes_on_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeConsumer({})
    monkeypatch.setattr(
        "loom.streaming.kafka.client._consumer._Consumer",
        lambda config: fake,
    )

    with KafkaConsumerClient(
        ConsumerSettings(brokers=("k1:9092",), group_id="g1", topics=("orders",)),
    ) as consumer:
        consumer.poll(100)

    assert fake.closed is True


def test_raw_consumer_context_manager_does_not_mask_body_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeConsumer({})
    fake.close_error = RuntimeError("close-boom")
    monkeypatch.setattr(
        "loom.streaming.kafka.client._consumer._Consumer",
        lambda config: fake,
    )

    with (
        pytest.raises(ValueError, match="body-boom"),
        KafkaConsumerClient(
            ConsumerSettings(brokers=("k1:9092",), group_id="g1", topics=("orders",)),
        ),
    ):
        raise ValueError("body-boom")
