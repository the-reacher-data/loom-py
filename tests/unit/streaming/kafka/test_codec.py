from __future__ import annotations

from loom.core.model import LoomFrozenStruct
from loom.streaming.kafka import MessageDescriptor, MsgspecCodec, build_message


class ProductEvent(LoomFrozenStruct):
    sku: str
    stock: int


def test_msgspec_codec_roundtrip_message_envelope() -> None:
    codec = MsgspecCodec[ProductEvent]()
    descriptor = MessageDescriptor(message_type="product.stock.updated", message_version=1)
    message = build_message(ProductEvent(sku="sku-1", stock=10), descriptor, produced_at_ms=100)

    raw = codec.encode(message)
    decoded = codec.decode(raw, ProductEvent)

    assert decoded == message
