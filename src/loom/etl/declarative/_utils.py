"""Shared internal utilities for ETL I/O builder classes.

Not part of the public API.
"""

from __future__ import annotations

from typing import TypeVar

_CopyT = TypeVar("_CopyT")


def _clone_slots(src: _CopyT, cls: type[_CopyT], slots: tuple[str, ...]) -> _CopyT:
    """Create a shallow copy of *src* by transferring all declared ``__slots__``."""
    new = object.__new__(cls)
    for slot in slots:
        object.__setattr__(new, slot, getattr(src, slot))
    return new
