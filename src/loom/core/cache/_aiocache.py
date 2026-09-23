"""The cache backend (``cache`` extra): the one module of ``loom.core.cache`` importing aiocache."""

from __future__ import annotations

from aiocache import caches  # type: ignore[import-untyped]

__all__ = ["caches"]
