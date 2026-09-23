"""``CacheGateway`` without the ``cache`` extra fails naming it, before touching a backend."""

from __future__ import annotations

import pytest

from loom.core.cache.gateway import CacheGateway
from loom.core.config.errors import ConfigError
from tests.helpers.extras import AIOCACHE_ISLAND, without_extra


def test_names_the_cache_extra_when_aiocache_is_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    without_extra(monkeypatch, ["aiocache"], [AIOCACHE_ISLAND])

    with pytest.raises(ConfigError, match=r"loom-kernel\[cache\]"):
        CacheGateway()
