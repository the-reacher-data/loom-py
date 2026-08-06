"""Unit tests for ``CacheGateway.apply_config`` alias validation."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from loom.core.cache.abc.config import CacheConfig
from loom.core.cache.gateway import CacheGateway
from loom.core.config.errors import ConfigError

_MEMORY = {"cache": "aiocache.SimpleMemoryCache"}


class TestApplyConfigAliasValidation:
    def test_a_declared_alias_missing_from_the_config_is_refused(self) -> None:
        config = CacheConfig(
            aiocache_alias="sessions",
            counter_alias="counters",
            aiocache_config={"sessions": dict(_MEMORY)},
        )
        with pytest.raises(ConfigError, match="'counters'"):
            CacheGateway.apply_config(config)

    def test_the_refusal_happens_before_aiocache_is_touched(self) -> None:
        config = CacheConfig(aiocache_alias="sessions", aiocache_config={})
        with (
            patch.object(CacheGateway, "configure") as configure,
            pytest.raises(ConfigError),
        ):
            CacheGateway.apply_config(config)
        configure.assert_not_called()

    def test_the_default_alias_keeps_its_sanctioned_fallback(self) -> None:
        with patch.object(CacheGateway, "configure") as configure:
            CacheGateway.apply_config(CacheConfig(aiocache_config={}))
        assert "default" in configure.call_args.args[0]
