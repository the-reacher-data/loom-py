from __future__ import annotations

from loom.core.model import LoomFrozenStruct
from loom.core.tracing import reset_trace_id, set_trace_id
from loom.streaming.kafka import ContentType, MessageDescriptor, SchemaRef, build_message


class OrderCreated(LoomFrozenStruct):
    order_id: str
    amount: int


def test_build_message_uses_active_trace_context() -> None:
    token = set_trace_id("trace-123")
    try:
        descriptor = MessageDescriptor(
            message_type="order.created",
            message_version=1,
            schema_ref=SchemaRef(
                namespace="orders",
                name="order.created",
                version="1",
                format="loom-msgpack",
            ),
        )
        message = build_message(
            OrderCreated(order_id="o-1", amount=5),
            descriptor,
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


def test_build_message_allows_explicit_trace_override() -> None:
    descriptor = MessageDescriptor(message_type="order.created", message_version=2)

    message = build_message(
        OrderCreated(order_id="o-2", amount=7),
        descriptor,
        trace_id="trace-explicit",
    )

    assert message.meta.trace_id == "trace-explicit"
    assert message.payload.order_id == "o-2"
