"""Unit tests for _batch_key — partition-aware key derivation.

These tests verify that Bytewax batch grouping uses transport metadata from
``MessageMeta`` to preserve Kafka partition-level parallelism, and falls back
to a fixed sentinel only when no metadata is available.
"""

from __future__ import annotations

from loom.core.model import LoomStruct
from loom.streaming.bytewax._adapter import _batch_key
from loom.streaming.core._message import Message, MessageMeta


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

    def test_topic_and_partition_produce_compound_key(self) -> None:
        assert _batch_key(_msg(topic="orders.in", partition=2)) == "orders.in:2"

    def test_partition_zero_is_not_treated_as_falsy(self) -> None:
        assert _batch_key(_msg(topic="events", partition=0)) == "events:0"

    def test_missing_topic_falls_back_to_default_prefix(self) -> None:
        assert _batch_key(_msg(partition=1)) == "default:1"

    def test_str_key_used_when_no_partition(self) -> None:
        assert _batch_key(_msg(key="tenant-a")) == "tenant-a"

    def test_bytes_key_decoded_when_no_partition(self) -> None:
        assert _batch_key(_msg(key=b"tenant-b")) == "tenant-b"

    def test_loom_sentinel_when_no_metadata(self) -> None:
        assert _batch_key(_msg()) == "loom"

    def test_loom_sentinel_for_non_message_items(self) -> None:
        assert _batch_key("not-a-message") == "loom"
        assert _batch_key(42) == "loom"
        assert _batch_key(None) == "loom"
