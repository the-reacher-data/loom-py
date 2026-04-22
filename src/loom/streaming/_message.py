"""Transport-neutral streaming message model."""

from __future__ import annotations

from typing import Generic, TypeVar

import msgspec

from loom.core.model import LoomFrozenStruct, LoomStruct

PayloadT = TypeVar("PayloadT", bound=LoomStruct | LoomFrozenStruct)


class MessageMeta(LoomFrozenStruct, frozen=True, kw_only=True):
    """Transport-neutral metadata for one logical event.

    Args:
        message_id: Stable event identifier.
        correlation_id: Optional correlation identifier.
        trace_id: Optional trace identifier.
        topic: Source topic name when available.
        partition: Source partition when available.
        offset: Source offset when available.
        key: Optional transport key when available.
        headers: Opaque transport headers.
    """

    message_id: str
    correlation_id: str | None = None
    trace_id: str | None = None
    topic: str | None = None
    partition: int | None = None
    offset: int | None = None
    key: bytes | str | None = None
    headers: dict[str, bytes] = msgspec.field(default_factory=dict)


class Message(LoomFrozenStruct, Generic[PayloadT], frozen=True, kw_only=True):
    """Logical typed streaming event.

    Args:
        payload: Typed event payload.
        meta: Transport-neutral metadata.
    """

    payload: PayloadT
    meta: MessageMeta


__all__ = ["Message", "MessageMeta"]
