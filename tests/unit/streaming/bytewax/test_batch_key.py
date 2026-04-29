"""Unit tests for _batch_key — partition-aware key derivation.

These tests verify that Bytewax batch grouping uses transport metadata from
``MessageMeta`` to preserve Kafka partition-level parallelism, and falls back
to a fixed sentinel only when no metadata is available.
"""

from __future__ import annotations

import pytest

from loom.core.model import LoomStruct
from loom.streaming.bytewax._adapter import _batch_key
from loom.streaming.core._message import Message, MessageMeta

pytestmark = pytest.mark.bytewax


def _msg(
    *,
    topic: str | None = None,
    partition: int | None = None,
    key: bytes | str | None = None,
) -> Message[LoomStruct]:
    """Build a message with optional transport metadata for key derivation tests."""

    class _Dummy(LoomStruct):
        v: str = ""

    return Message(
        payload=_Dummy(),
        meta=MessageMeta(
            message_id="x",
            topic=topic,
            partition=partition,
            key=key,
        ),
    )


class TestBatchKey:
    """_batch_key derives grouping keys from MessageMeta for partition-aware batching."""

    @pytest.mark.parametrize(
        ("message", "expected"),
        [
            (_msg(topic="orders.in", partition=2), "orders.in:2"),
            (_msg(topic="events", partition=0), "events:0"),
            (_msg(partition=1), "default:1"),
            (_msg(key="tenant-a"), "tenant-a"),
            (_msg(key=b"tenant-b"), "tenant-b"),
            (_msg(), "loom"),
            ("not-a-message", "loom"),
            (42, "loom"),
            (None, "loom"),
        ],
    )
    def test_batch_key_derivation(self, message: object, expected: str) -> None:
        assert _batch_key(message) == expected
