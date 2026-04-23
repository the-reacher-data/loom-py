"""Shared typing primitives for the streaming DSL."""

from __future__ import annotations

from typing_extensions import TypeAliasType

from loom.core.model import LoomFrozenStruct, LoomStruct

StreamPayload = TypeAliasType("StreamPayload", LoomStruct | LoomFrozenStruct)

__all__ = ["StreamPayload"]
