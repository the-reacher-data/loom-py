from __future__ import annotations

from typing import Any

import msgspec


class MsgpackCodec:
    """Codec that serializes and deserializes values using msgspec's MessagePack format."""

    def encode(self, value: Any) -> bytes:
        """Serialize a Python object to MessagePack bytes.

        Args:
            value: Object to encode.

        Returns:
            MessagePack-encoded bytes.
        """
        return msgspec.msgpack.encode(value)

    def decode(self, payload: bytes) -> Any:
        """Deserialize MessagePack bytes back into a Python object.

        Args:
            payload: MessagePack-encoded bytes.

        Returns:
            The decoded Python object.
        """
        return msgspec.msgpack.decode(payload)
