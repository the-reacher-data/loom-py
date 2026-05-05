"""Typed Kafka transport record."""

from __future__ import annotations

from typing import Generic, TypeVar

import msgspec

from loom.core.model import LoomFrozenStruct

PayloadT = TypeVar("PayloadT")


class KafkaRecord(LoomFrozenStruct, Generic[PayloadT], frozen=True):
    """Typed Kafka transport record.

    Attributes:
        topic: Kafka topic name.
        key: Transport key used for partitioning.
        value: Typed record value.
        headers: Kafka headers.
        partition: Partition number when known.
        offset: Offset when known.
        timestamp_ms: Broker or producer timestamp in epoch milliseconds.
    """

    topic: str
    key: bytes | str | None
    value: PayloadT
    headers: dict[str, bytes] = msgspec.field(default_factory=dict)
    partition: int | None = None
    offset: int | None = None
    timestamp_ms: int | None = None
