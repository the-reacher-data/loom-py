from __future__ import annotations

from typing import Any

import pytest

from loom.streaming import ErrorKind
from loom.streaming.kafka import (
    DecodeError,
    DecodeOk,
    KafkaRecord,
    MsgspecCodec,
    try_decode_record,
)
from tests.unit.streaming.kafka.cases import OrderCreated

pytestmark = pytest.mark.kafka


class TestKafkaWireDecode:
    def test_try_decode_record_maps_envelope_to_stream_message(
        self,
        order_created_envelope_with_metadata: Any,
    ) -> None:
        codec = MsgspecCodec[OrderCreated]()
        record = KafkaRecord(
            topic="orders.events",
            key=b"tenant-a",
            value=codec.encode(order_created_envelope_with_metadata),
            headers={"content-type": b"application/x-loom-msgpack"},
            partition=3,
            offset=9,
            timestamp_ms=1235,
        )

        result = try_decode_record(record, OrderCreated, codec)

        assert isinstance(result, DecodeOk)
        assert result.message.payload == OrderCreated(order_id="o-1", amount=5)
        assert result.message.meta.message_id == "orders.events:3:9"
        assert result.message.meta.correlation_id == "corr-1"
        assert result.message.meta.trace_id == "trace-1"
        assert result.message.meta.causation_id == "cause-1"
        assert result.message.meta.produced_at_ms == 1234
        assert result.message.meta.message_type == "order.created"
        assert result.message.meta.message_version == 1
        assert result.message.meta.topic == "orders.events"
        assert result.message.meta.partition == 3
        assert result.message.meta.offset == 9
        assert result.message.meta.key == b"tenant-a"
        assert result.message.meta.headers == {"content-type": b"application/x-loom-msgpack"}

    def test_try_decode_record_returns_wire_error_without_raising(self) -> None:
        codec = MsgspecCodec[OrderCreated]()
        record = KafkaRecord(
            topic="orders.events",
            key="tenant-a",
            value=b"not-msgpack",
            headers={"content-type": b"application/x-loom-msgpack"},
            partition=1,
            offset=7,
            timestamp_ms=999,
        )

        result = try_decode_record(record, OrderCreated, codec)

        assert isinstance(result, DecodeError)
        assert result.error.kind is ErrorKind.WIRE
        assert result.error.original_message is None
        assert result.raw == b"not-msgpack"
        assert result.topic == "orders.events"
        assert result.partition == 1
        assert result.offset == 7
        assert result.key == b"tenant-a"
        assert result.headers == {"content-type": b"application/x-loom-msgpack"}
        assert result.timestamp_ms == 999
