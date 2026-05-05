"""Multi-type dispatch wire decode tests."""

from __future__ import annotations

import pytest

from loom.core.model import LoomFrozenStruct
from loom.streaming import ErrorKind
from loom.streaming.core._errors import ErrorEnvelope, ErrorMessage, ErrorMessageMeta
from loom.streaming.kafka import (
    DecodeError,
    DecodeOk,
    KafkaRecord,
    MessageDescriptor,
    MsgspecCodec,
    build_message,
)

pytestmark = pytest.mark.kafka


class _OrderEvent(LoomFrozenStruct, frozen=True):
    order_id: str


class _ProductEvent(LoomFrozenStruct, frozen=True):
    sku: str
    stock: int


_ORDER_MT = "order.event"
_PRODUCT_MT = "product.event"
_ERROR_MT_TASK = "loom.streaming.error.task"


def _encode_plain(payload: LoomFrozenStruct, message_type: str) -> bytes:
    codec: MsgspecCodec[LoomFrozenStruct] = MsgspecCodec()
    descriptor = MessageDescriptor(message_type=message_type, message_version=1)
    envelope = build_message(payload, descriptor, produced_at_ms=1000)
    return codec.encode(envelope)


def _encode_error(
    payload: LoomFrozenStruct,
    inner_message_type: str,
    kind: ErrorKind = ErrorKind.TASK,
) -> bytes:
    meta = ErrorMessageMeta(message_id="src-1", message_type=inner_message_type)
    original = ErrorMessage(payload=payload, meta=meta)
    error_envelope = ErrorEnvelope(
        kind=kind,
        reason="processing failed",
        payload_type=inner_message_type,
        original_message=original,
    )
    codec: MsgspecCodec[ErrorEnvelope[LoomFrozenStruct]] = MsgspecCodec()
    descriptor = MessageDescriptor(
        message_type=f"loom.streaming.error.{kind.value}",
        message_version=1,
    )
    envelope = build_message(error_envelope, descriptor, produced_at_ms=1000)
    return codec.encode(envelope)


def _record(value: bytes, topic: str = "events.all") -> KafkaRecord[bytes]:
    return KafkaRecord(topic=topic, key=b"k", value=value, partition=0, offset=1)


class TestTryDecodeMultiRecord:
    def test_dispatches_first_type_by_message_type(self) -> None:
        from loom.streaming.kafka import DispatchTable, try_decode_multi_record

        dispatch = DispatchTable(
            plain={_ORDER_MT: _OrderEvent, _PRODUCT_MT: _ProductEvent},
            error={},
        )
        codec: MsgspecCodec[LoomFrozenStruct] = MsgspecCodec()
        record = _record(_encode_plain(_OrderEvent(order_id="o-1"), _ORDER_MT))

        result = try_decode_multi_record(record, dispatch, codec)

        assert isinstance(result, DecodeOk)
        assert isinstance(result.message.payload, _OrderEvent)
        assert result.message.payload.order_id == "o-1"

    def test_dispatches_second_type_by_message_type(self) -> None:
        from loom.streaming.kafka import DispatchTable, try_decode_multi_record

        dispatch = DispatchTable(
            plain={_ORDER_MT: _OrderEvent, _PRODUCT_MT: _ProductEvent},
            error={},
        )
        codec: MsgspecCodec[LoomFrozenStruct] = MsgspecCodec()
        record = _record(_encode_plain(_ProductEvent(sku="sku-1", stock=10), _PRODUCT_MT))

        result = try_decode_multi_record(record, dispatch, codec)

        assert isinstance(result, DecodeOk)
        assert isinstance(result.message.payload, _ProductEvent)
        assert result.message.payload.sku == "sku-1"

    def test_unknown_message_type_returns_wire_error(self) -> None:
        from loom.streaming.kafka import DispatchTable, try_decode_multi_record

        dispatch = DispatchTable(
            plain={_ORDER_MT: _OrderEvent},
            error={},
        )
        codec: MsgspecCodec[LoomFrozenStruct] = MsgspecCodec()
        record = _record(_encode_plain(_ProductEvent(sku="sku-1", stock=5), _PRODUCT_MT))

        result = try_decode_multi_record(record, dispatch, codec)

        assert isinstance(result, DecodeError)
        assert result.error.kind is ErrorKind.WIRE
        assert _PRODUCT_MT in result.error.reason

    def test_malformed_bytes_for_known_type_returns_wire_error(self) -> None:
        from loom.streaming.kafka import DispatchTable, try_decode_multi_record

        dispatch = DispatchTable(
            plain={_ORDER_MT: _OrderEvent},
            error={},
        )
        codec: MsgspecCodec[LoomFrozenStruct] = MsgspecCodec()
        record = _record(b"not-msgpack")

        result = try_decode_multi_record(record, dispatch, codec)

        assert isinstance(result, DecodeError)
        assert result.error.kind is ErrorKind.WIRE

    def test_dispatches_error_envelope_by_inner_payload_type(self) -> None:
        from loom.streaming.kafka import DispatchTable, try_decode_multi_record

        dispatch = DispatchTable(
            plain={},
            error={_ORDER_MT: ErrorEnvelope[_OrderEvent]},
        )
        codec: MsgspecCodec[LoomFrozenStruct] = MsgspecCodec()
        record = _record(
            _encode_error(_OrderEvent(order_id="o-2"), _ORDER_MT),
            topic="errors.all",
        )

        result = try_decode_multi_record(record, dispatch, codec)

        assert isinstance(result, DecodeOk)
        assert isinstance(result.message.payload, ErrorEnvelope)
        assert result.message.payload.payload_type == _ORDER_MT
        assert result.message.payload.kind is ErrorKind.TASK

    def test_distinguishes_two_error_envelope_variants(self) -> None:
        from loom.streaming.kafka import DispatchTable, try_decode_multi_record

        dispatch = DispatchTable(
            plain={},
            error={
                _ORDER_MT: ErrorEnvelope[_OrderEvent],
                _PRODUCT_MT: ErrorEnvelope[_ProductEvent],
            },
        )
        codec: MsgspecCodec[LoomFrozenStruct] = MsgspecCodec()

        order_record = _record(
            _encode_error(_OrderEvent(order_id="o-3"), _ORDER_MT), topic="errors.all"
        )
        product_record = _record(
            _encode_error(_ProductEvent(sku="s-1", stock=3), _PRODUCT_MT), topic="errors.all"
        )

        order_result = try_decode_multi_record(order_record, dispatch, codec)
        product_result = try_decode_multi_record(product_record, dispatch, codec)

        assert isinstance(order_result, DecodeOk)
        assert order_result.message.payload.payload_type == _ORDER_MT

        assert isinstance(product_result, DecodeOk)
        assert product_result.message.payload.payload_type == _PRODUCT_MT

    def test_error_envelope_with_unknown_inner_type_returns_wire_error(self) -> None:
        from loom.streaming.kafka import DispatchTable, try_decode_multi_record

        dispatch = DispatchTable(
            plain={},
            error={_ORDER_MT: ErrorEnvelope[_OrderEvent]},
        )
        codec: MsgspecCodec[LoomFrozenStruct] = MsgspecCodec()
        record = _record(
            _encode_error(_ProductEvent(sku="s-1", stock=1), _PRODUCT_MT), topic="errors.all"
        )

        result = try_decode_multi_record(record, dispatch, codec)

        assert isinstance(result, DecodeError)
        assert result.error.kind is ErrorKind.WIRE

    def test_preserves_record_metadata_on_decode_ok(self) -> None:
        from loom.streaming.kafka import DispatchTable, try_decode_multi_record

        dispatch = DispatchTable(
            plain={_ORDER_MT: _OrderEvent},
            error={},
        )
        codec: MsgspecCodec[LoomFrozenStruct] = MsgspecCodec()
        record = KafkaRecord(
            topic="events.all",
            key=b"tenant-1",
            value=_encode_plain(_OrderEvent(order_id="o-4"), _ORDER_MT),
            partition=2,
            offset=5,
            timestamp_ms=9999,
        )

        result = try_decode_multi_record(record, dispatch, codec)

        assert isinstance(result, DecodeOk)
        assert result.message.meta.topic == "events.all"
        assert result.message.meta.partition == 2
        assert result.message.meta.offset == 5
        assert result.message.meta.key == b"tenant-1"

    def test_preserves_record_metadata_on_decode_error(self) -> None:
        from loom.streaming.kafka import DispatchTable, try_decode_multi_record

        dispatch = DispatchTable(plain={_ORDER_MT: _OrderEvent}, error={})
        codec: MsgspecCodec[LoomFrozenStruct] = MsgspecCodec()
        record = KafkaRecord(
            topic="events.all",
            key="k",
            value=b"garbage",
            partition=1,
            offset=7,
            timestamp_ms=111,
        )

        result = try_decode_multi_record(record, dispatch, codec)

        assert isinstance(result, DecodeError)
        assert result.raw == b"garbage"
        assert result.topic == "events.all"
        assert result.partition == 1
        assert result.offset == 7
