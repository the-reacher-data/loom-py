"""Routing expression helpers."""

from __future__ import annotations

from loom.core.expr import RootRef

msg = RootRef("message")
"""Streaming message expression root for transport metadata.

Examples:
    ``msg.meta.topic == "urls"``
    ``msg.meta.headers["risk"] == b"high"``
"""

payload = RootRef("payload")
"""Streaming payload expression root.

Resolves directly against the domain object carried by the message,
without navigating through the ``Message`` envelope.

Examples:
    ``payload.cache_hit == True``
    ``payload.status.isin(["ok", "retry"])``
"""

__all__ = ["msg", "payload"]
