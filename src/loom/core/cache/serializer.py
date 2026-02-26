from __future__ import annotations

import importlib
from typing import Any

import msgspec

_ = importlib.import_module("aiocache.serializers")


class MsgspecSerializer:
    """aiocache serializer backed by msgspec msgpack."""
    encoding: str | None = None

    def dumps(self, value: Any) -> bytes:
        """Serialize a Python object to MessagePack bytes for cache storage.

        Args:
            value: Object to serialize.

        Returns:
            MessagePack-encoded bytes.
        """
        return msgspec.msgpack.encode(value)

    def loads(self, value: bytes | None) -> Any:
        """Deserialize MessagePack bytes back into a Python object.

        Args:
            value: Raw bytes from cache, or ``None``.

        Returns:
            The decoded Python object, or ``None`` if input is ``None``.
        """
        if value is None:
            return None
        return msgspec.msgpack.decode(value)
