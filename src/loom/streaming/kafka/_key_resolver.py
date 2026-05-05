"""Partition-key resolver contracts and built-ins."""

from __future__ import annotations

from typing import Generic, Protocol, TypeVar

from loom.core.model import LoomFrozenStruct
from loom.streaming.kafka._record import KafkaRecord

PayloadT = TypeVar("PayloadT")


class PartitionKeyResolver(Protocol[PayloadT]):
    """Compute the Kafka partition key for an outgoing record."""

    def resolve(self, record: KafkaRecord[PayloadT]) -> bytes | None:
        """Return Kafka partition-key bytes.

        Args:
            record: Typed Kafka record to inspect.

        Returns:
            Partition-key bytes or ``None`` for broker-controlled routing.
        """


class PreserveKey(Generic[PayloadT]):
    """Preserve an existing record key when present."""

    def resolve(self, record: KafkaRecord[PayloadT]) -> bytes | None:
        """Return the existing record key encoded as bytes.

        Args:
            record: Typed Kafka record.

        Returns:
            Existing key bytes or ``None``.
        """

        key = record.key
        if key is None:
            return None
        if isinstance(key, bytes):
            return key
        return key.encode("utf-8")


class FixedKey(LoomFrozenStruct, frozen=True):
    """Always use one fixed partition key."""

    value: bytes

    def resolve(self, record: object) -> bytes:
        """Return the fixed key.

        Args:
            record: Kafka record or compatible object. Its payload is ignored.

        Returns:
            Fixed key bytes.
        """

        del record
        return self.value
