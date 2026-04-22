"""Logical stream shapes and explicit shape adapters."""

from __future__ import annotations

from enum import StrEnum

from loom.core.model import LoomFrozenStruct


class StreamShape(StrEnum):
    """Logical data shape at a streaming graph edge."""

    RECORD = "record"
    MANY = "many"
    BATCH = "batch"
    NONE = "none"


class ForEach(LoomFrozenStruct, frozen=True):
    """Explicit shape adapter from ``batch`` to ``record``."""


class CollectBatch(LoomFrozenStruct, frozen=True):
    """Explicit shape adapter from ``record`` to ``batch``.

    Args:
        max_records: Maximum records collected into one batch.
        timeout_ms: Maximum wait time before materializing a partial batch.
    """

    max_records: int
    timeout_ms: int

    def __post_init__(self) -> None:
        """Validate batch collection limits."""
        if self.max_records < 1:
            raise ValueError("CollectBatch.max_records must be greater than zero.")
        if self.timeout_ms < 1:
            raise ValueError("CollectBatch.timeout_ms must be greater than zero.")


class Drain(LoomFrozenStruct, frozen=True):
    """Explicit terminal adapter from any shape to ``none``."""


__all__ = ["CollectBatch", "Drain", "ForEach", "StreamShape"]
