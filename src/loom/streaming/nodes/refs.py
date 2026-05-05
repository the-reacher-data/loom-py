"""Public expression roots for the streaming DSL."""

from __future__ import annotations

from loom.core.expr import RootRef

msg = RootRef("message")
"""Streaming message expression root for transport metadata."""

payload = RootRef("payload")
"""Streaming payload expression root for the domain object carried by a message."""

__all__ = ["msg", "payload"]
