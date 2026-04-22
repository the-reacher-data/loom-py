"""Raw Kafka transport protocols."""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

from loom.streaming.kafka._errors import KafkaDeliveryError
from loom.streaming.kafka._record import KafkaRecord

DeliveryCallback = Callable[[KafkaRecord[bytes], KafkaDeliveryError | None], None]


class KafkaProducer(Protocol):
    """Raw Kafka producer contract.

    Sends ``KafkaRecord[bytes]`` to Kafka. No codec, no envelope,
    no serialization — only raw byte transport.
    """

    def send(self, record: KafkaRecord[bytes]) -> None:
        """Produce one raw byte record.

        Args:
            record: Kafka record with a ``bytes`` value.
        """

    def flush(self, timeout_ms: int | None = None) -> None:
        """Flush pending records.

        Args:
            timeout_ms: Optional maximum flush wait in milliseconds.
        """

    def close(self) -> None:
        """Flush and close the producer."""


class KafkaConsumer(Protocol):
    """Raw Kafka consumer contract.

    Returns ``KafkaRecord[bytes]`` from Kafka. No codec, no envelope,
    no deserialization — only raw byte transport.
    """

    def poll(self, timeout_ms: int) -> KafkaRecord[bytes] | None:
        """Read one raw byte record.

        Args:
            timeout_ms: Maximum poll wait in milliseconds.

        Returns:
            One raw Kafka record or ``None`` when no record is available.
        """

    def commit(self, *, asynchronous: bool = False) -> None:
        """Commit consumed offsets.

        Args:
            asynchronous: Whether the backend may commit asynchronously.
        """

    def close(self) -> None:
        """Close the consumer and release resources."""
