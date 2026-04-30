"""Shared Kafka test doubles for streaming Kafka tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from loom.streaming.kafka import KafkaRecord
from loom.streaming.kafka._config import ConsumerSettings


class ProducerBackendStub:
    """In-memory confluent-like producer stub."""

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


class FakeDeliveryError:
    """Fake delivery error emitted by the producer callback."""

    def __str__(self) -> str:
        return "delivery-boom"


class FakeKafkaMessage:
    """Kafka-like message returned by the raw consumer stub."""

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


class ConsumerBackendStub:
    """In-memory confluent-like consumer stub."""

    def __init__(self, config: dict[str, str]) -> None:
        self.config = config
        self.subscribed: list[str] = []
        self.next_message: Any | None = None
        self.closed = False
        self.poll_calls: list[float] = []
        self.commit_calls: list[bool] = []
        self.commit_error: Exception | None = None
        self.close_error: Exception | None = None

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed = topics

    def poll(self, timeout: float) -> FakeKafkaMessage | None:
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


class RawProducerStub:
    """In-memory raw producer for message-level and runtime sink tests."""

    def __init__(self) -> None:
        self.sent: list[KafkaRecord[bytes]] = []
        self.flushed = False
        self.closed = False
        self.close_error: Exception | None = None
        self.flush_error: Exception | None = None

    def send(self, record: KafkaRecord[bytes]) -> None:
        self.sent.append(record)

    def flush(self, timeout_ms: int | None = None) -> None:
        del timeout_ms
        self.flushed = True
        if self.flush_error is not None:
            raise self.flush_error

    def close(self) -> None:
        if self.close_error is not None:
            raise self.close_error
        self.closed = True


class RawConsumerStub:
    """In-memory raw consumer for message-level tests."""

    def __init__(self, records: list[KafkaRecord[bytes] | None] | None = None) -> None:
        self._records = list(records or [])
        self.closed = False
        self.commit_calls: list[bool] = []
        self.close_error: Exception | None = None

    def load_records(self, records: list[KafkaRecord[bytes] | None]) -> None:
        """Replace the queued records consumed by the stub."""
        self._records = list(records)

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


class RuntimeConsumerStub:
    """In-memory raw consumer for runtime source tests."""

    def __init__(self, settings: ConsumerSettings) -> None:
        del settings
        self.closed = False
        self.poll_calls: list[int] = []
        self.commit_calls: list[bool] = []
        self.close_error: Exception | None = None
        self.next_message: Any | None = None

    def poll(self, timeout_ms: int) -> object | None:
        self.poll_calls.append(timeout_ms)
        return self.next_message

    def commit(self, *, asynchronous: bool = False) -> None:
        self.commit_calls.append(asynchronous)

    def close(self) -> None:
        if self.close_error is not None:
            raise self.close_error
        self.closed = True


@dataclass(slots=True)
class ProducerBackendInstaller:
    """Callable installer that captures the raw producer stub created by Kafka."""

    stub: ProducerBackendStub = field(default_factory=lambda: ProducerBackendStub({}))

    def __call__(self, config: dict[str, str]) -> ProducerBackendStub:
        self.stub.config = config
        return self.stub


@dataclass(slots=True)
class ConsumerBackendInstaller:
    """Callable installer that captures the raw consumer stub created by Kafka."""

    stub: ConsumerBackendStub = field(default_factory=lambda: ConsumerBackendStub({}))

    def __call__(self, config: dict[str, str]) -> ConsumerBackendStub:
        self.stub.config = config
        return self.stub


def install_raw_producer_stub(
    monkeypatch: pytest.MonkeyPatch,
    installer: ProducerBackendInstaller | None = None,
) -> ProducerBackendInstaller:
    """Install a raw producer stub into the Kafka client module and return the installer."""
    producer_installer = installer or ProducerBackendInstaller()
    monkeypatch.setattr(
        "loom.streaming.kafka.client._producer._Producer",
        producer_installer,
    )
    return producer_installer


def install_raw_consumer_stub(
    monkeypatch: pytest.MonkeyPatch,
    installer: ConsumerBackendInstaller | None = None,
) -> ConsumerBackendInstaller:
    """Install a raw consumer stub into the Kafka client module and return the installer."""
    consumer_installer = installer or ConsumerBackendInstaller()
    monkeypatch.setattr(
        "loom.streaming.kafka.client._consumer._Consumer",
        consumer_installer,
    )
    return consumer_installer
