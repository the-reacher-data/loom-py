"""Routing expression helpers."""

from __future__ import annotations

from loom.core.expr import RootRef

msg = RootRef("message")
"""Streaming message expression root.

Examples:
    ``msg.payload.country == "ES"``
    ``msg.meta.headers["risk"] == b"high"``
"""

__all__ = ["msg"]
