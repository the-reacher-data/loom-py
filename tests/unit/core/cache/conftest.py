"""Fixtures shared by the cache suites."""

from __future__ import annotations

import pytest

from loom.core.cache import CacheConfig


@pytest.fixture
def cache_config() -> CacheConfig:
    """Cache configuration with short, explicit TTLs."""
    return CacheConfig(enabled=True, default_ttl=100, default_list_ttl=50)
