from __future__ import annotations

import pytest

from loom.streaming.kafka import FixedKey, KafkaRecord, PartitionKeyResolver, PreserveKey

pytestmark = pytest.mark.kafka


def test_preserve_key_returns_encoded_string_key() -> None:
    resolver = PreserveKey[int]()
    record = KafkaRecord(topic="orders", key="tenant-a", value=1)

    assert resolver.resolve(record) == b"tenant-a"


def test_preserve_key_returns_none_when_missing() -> None:
    resolver = PreserveKey[int]()
    record = KafkaRecord(topic="orders", key=None, value=1)

    assert resolver.resolve(record) is None


def test_fixed_key_always_returns_same_value() -> None:
    resolver: PartitionKeyResolver[int] = FixedKey(value=b"fixed")
    record = KafkaRecord(topic="orders", key=None, value=1)

    assert resolver.resolve(record) == b"fixed"
