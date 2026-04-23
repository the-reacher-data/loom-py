"""Streaming error model for explicit error branches."""

from __future__ import annotations

from enum import StrEnum
from typing import Generic, TypeVar

from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.streaming.core._message import Message

PayloadT = TypeVar("PayloadT", bound=LoomStruct | LoomFrozenStruct)


class ErrorKind(StrEnum):
    """Logical categories for streaming flow errors."""

    WIRE = "wire"
    ROUTING = "routing"
    TASK = "task"
    BUSINESS = "business"


class ErrorEnvelope(LoomFrozenStruct, Generic[PayloadT], frozen=True):
    """Structured error payload routed through explicit error branches.

    Args:
        kind: Logical error category.
        reason: Human-readable reason.
        original_message: Original message when available.
    """

    kind: ErrorKind
    reason: str
    original_message: Message[PayloadT] | None = None


__all__ = ["ErrorEnvelope", "ErrorKind"]
