"""Unit tests for ``CacheGateway.apply_config`` alias handling."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from loom.core.cache.abc.config import CacheConfig
from loom.core.cache.gateway import CacheGateway
from loom.core.config.errors import ConfigError


def _config(**overrides: object) -> CacheConfig:
    params: dict = {
        "aiocache_alias": "sessions",
        "counter_alias": "counters",
        "aiocache_config": {
            "sessions": {"cache": "aiocache.SimpleMemoryCache"},
            "counters": {"cache": "aiocache.SimpleMemoryCache"},
        },
    }
    params.update(overrides)
    return CacheConfig(**params)


class TestApplyConfigAliases:
    def test_injects_the_default_alias_aiocache_demands(self) -> None:
        with patch.object(CacheGateway, "configure") as configure:
            CacheGateway.apply_config(_config())
        raw = configure.call_args.args[0]
        assert raw["default"] == {"cache": "aiocache.SimpleMemoryCache"}

    def test_keeps_a_declared_default(self) -> None:
        config = _config(
            aiocache_config={
                "default": {"cache": "aiocache.SimpleMemoryCache", "ttl": 5},
                "sessions": {"cache": "aiocache.SimpleMemoryCache"},
                "counters": {"cache": "aiocache.SimpleMemoryCache"},
            }
        )
        with patch.object(CacheGateway, "configure") as configure:
            CacheGateway.apply_config(config)
        assert configure.call_args.args[0]["default"]["ttl"] == 5

    def test_declared_alias_missing_from_config_is_refused(self) -> None:
        config = _config(aiocache_config={"sessions": {"cache": "aiocache.SimpleMemoryCache"}})
        with pytest.raises(ConfigError, match="'counters'"):
            CacheGateway.apply_config(config)
