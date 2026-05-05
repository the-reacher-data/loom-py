"""Codec protocol and MessagePack implementation for Kafka messages."""

from __future__ import annotations

from functools import lru_cache
from typing import Generic, Protocol, TypeVar

import msgspec

from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.streaming.kafka._message import MessageEnvelope

PayloadT = TypeVar("PayloadT", bound=LoomStruct | LoomFrozenStruct)


class KafkaCodec(Protocol[PayloadT]):
    """Encode and decode Kafka message envelopes."""

    def encode(self, message: MessageEnvelope[PayloadT]) -> bytes:
        """Serialize one message envelope to bytes.

        Args:
            message: Typed message envelope.

        Returns:
            Encoded message bytes.
        """
        ...

    def decode(self, raw: bytes, payload_type: type[PayloadT]) -> MessageEnvelope[PayloadT]:
        """Deserialize bytes to one message envelope.

        Args:
            raw: Raw Kafka payload bytes.
            payload_type: Expected payload model type.

        Returns:
            Decoded typed message envelope.
        """
        ...


class MsgspecCodec(Generic[PayloadT]):
    """Direct MessagePack codec for Kafka message envelopes."""

    def encode(self, message: MessageEnvelope[PayloadT]) -> bytes:
        """Serialize one typed message envelope.

        Args:
            message: Typed message envelope.

        Returns:
            MessagePack bytes.
        """
        return msgspec.msgpack.encode(message)

    def decode(self, raw: bytes, payload_type: type[PayloadT]) -> MessageEnvelope[PayloadT]:
        """Deserialize one typed message envelope.

        Args:
            raw: Raw MessagePack bytes.
            payload_type: Expected payload model type.

        Returns:
            Decoded typed message envelope.
        """
        envelope_type = _envelope_type(payload_type)  # type: ignore[arg-type]
        decoded: MessageEnvelope[PayloadT] = msgspec.msgpack.decode(raw, type=envelope_type)
        return decoded


@lru_cache(maxsize=128)
def _envelope_type(payload_type: type[PayloadT]) -> object:
    """Return the parametrised msgspec envelope type, cached per payload class."""
    return MessageEnvelope[payload_type]  # type: ignore[valid-type]
