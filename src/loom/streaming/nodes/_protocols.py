"""Routing protocols for custom user logic."""

from __future__ import annotations

from typing import Protocol, TypeVar, runtime_checkable

from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.streaming.core._message import Message

PayloadT = TypeVar("PayloadT", bound=LoomStruct | LoomFrozenStruct)


@runtime_checkable
class Selector(Protocol[PayloadT]):
    """Custom route-key selector."""

    def select(self, message: Message[PayloadT]) -> object:
        """Return a route key for *message*."""
        ...


@runtime_checkable
class Predicate(Protocol[PayloadT]):
    """Custom boolean route predicate."""

    def matches(self, message: Message[PayloadT]) -> bool:
        """Return whether *message* matches this predicate."""
        ...


__all__ = ["Predicate", "Selector"]
