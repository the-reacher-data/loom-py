"""Errors raised by the cache layer."""

from __future__ import annotations


class CacheWriteError(ValueError):
    """A value could not be serialised for the cache; nothing was stored."""
