from __future__ import annotations

import pytest

from loom.core.tracing import reset_trace_id, set_trace_id
from loom.streaming.core._errors import ErrorEnvelope, ErrorKind, snapshot_message
from loom.streaming.core._message import Message, MessageMeta
from loom.streaming.kafka import (
    ContentType,
    MessageDescriptor,
    MessageEnvelope,
    MsgspecCodec,
    build_message,
)
from tests.unit.streaming.kafka.cases import OrderCreated, ProductEvent

pytestmark = pytest.mark.kafka


class TestMessageEnvelope:
    def test_build_message_uses_active_trace_context(
        self,
        order_created_payload: OrderCreated,
        order_created_descriptor_v1: MessageDescriptor,
    ) -> None:
        token = set_trace_id("trace-123")
        try:
            message = build_message(
                order_created_payload,
                order_created_descriptor_v1,
                correlation_id="corr-1",
                causation_id="cause-1",
                produced_at_ms=42,
            )
        finally:
            reset_trace_id(token)

        assert message.meta.trace_id == "trace-123"
        assert message.meta.correlation_id == "corr-1"
        assert message.meta.causation_id == "cause-1"
        assert message.meta.produced_at_ms == 42
        assert message.meta.descriptor.content_type == ContentType.msgpack()

    def test_build_message_allows_explicit_trace_override(
        self,
        order_created_descriptor_v2: MessageDescriptor,
    ) -> None:
        token = set_trace_id("trace-123")
        try:
            message = build_message(
                OrderCreated(order_id="o-2", amount=7),
                order_created_descriptor_v2,
                trace_id="trace-explicit",
            )
        finally:
            reset_trace_id(token)

        assert message.meta.trace_id == "trace-explicit"
        assert message.payload.order_id == "o-2"

    def test_build_message_generates_trace_when_context_is_missing(
        self,
        monkeypatch: pytest.MonkeyPatch,
        order_created_descriptor_v1: MessageDescriptor,
    ) -> None:
        monkeypatch.setattr("loom.streaming.kafka._message.generate_trace_id", lambda: "trace-new")

        message = build_message(
            OrderCreated(order_id="o-3", amount=9),
            order_created_descriptor_v1,
        )

        assert message.meta.trace_id == "trace-new"


class TestMsgspecCodec:
    def test_roundtrip_message_envelope(
        self,
        product_event_envelope: MessageEnvelope[ProductEvent],
    ) -> None:
        codec = MsgspecCodec[ProductEvent]()
        raw = codec.encode(product_event_envelope)
        decoded = codec.decode(raw, ProductEvent)

        assert decoded == product_event_envelope

    def test_roundtrip_error_envelope(
        self,
        order_created_payload: OrderCreated,
        order_created_descriptor_v1: MessageDescriptor,
    ) -> None:
        codec = MsgspecCodec[ErrorEnvelope[OrderCreated]]()
        original = Message(
            payload=order_created_payload,
            meta=MessageMeta(
                message_id="msg-1",
                key=b"tenant-a",
            ),
        )
        envelope = build_message(
            ErrorEnvelope(
                kind=ErrorKind.TASK,
                reason="boom",
                original_message=snapshot_message(original),
            ),
            order_created_descriptor_v1,
        )

        raw = codec.encode(envelope)
        decoded = codec.decode(raw, ErrorEnvelope[OrderCreated])

        assert decoded.payload.kind is ErrorKind.TASK
        assert decoded.payload.reason == "boom"
        assert decoded.payload.original_message is not None
        assert decoded.payload.original_message.payload == order_created_payload
        assert decoded.payload.original_message.meta.key == b"tenant-a"
