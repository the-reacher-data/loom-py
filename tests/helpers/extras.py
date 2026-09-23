"""Simulate an optional extra that is absent, or replaced by a stub, for one test.

An island imports its SDK at the top and is cached in ``sys.modules`` once
loaded, so blocking the SDK alone would not affect a test that runs after the
island was imported. Both helpers therefore evict the island as well: the next
``import_optional`` re-imports it against the blocked or stubbed SDK.
``sys.modules[name] = None`` is what makes ``import name`` raise
``ImportError``; ``monkeypatch`` restores every entry afterwards.
"""

from __future__ import annotations

import sys
from collections.abc import Iterable, Mapping

import pytest

BOTO3_ISLAND = "loom.core.config._boto3"
AIOCACHE_ISLAND = "loom.core.cache._aiocache"
SCRAPE_ISLAND = "loom.prometheus.scrape"
ENGINE_EXTRAS = "loom.ai.engines.pydantic_ai.extras"
ENGINE_PROVIDERS = "loom.ai.engines.pydantic_ai.providers"


def without_extra(
    monkeypatch: pytest.MonkeyPatch, sdk_modules: Iterable[str], islands: Iterable[str]
) -> None:
    """Make *sdk_modules* unimportable and evict *islands* so they re-import against that."""
    for name in sdk_modules:
        monkeypatch.setitem(sys.modules, name, None)
    for island in islands:
        monkeypatch.setitem(sys.modules, island, None)


def with_stubbed_extra(
    monkeypatch: pytest.MonkeyPatch, stubs: Mapping[str, object], islands: Iterable[str]
) -> None:
    """Serve *stubs* as the SDK modules and evict *islands* so they import the stubs."""
    for name, stub in stubs.items():
        monkeypatch.setitem(sys.modules, name, stub)
    for island in islands:
        monkeypatch.delitem(sys.modules, island, raising=False)


def without_boto3(monkeypatch: pytest.MonkeyPatch) -> None:
    """The ``config-ssm`` extra is absent."""
    without_extra(monkeypatch, ["boto3"], [BOTO3_ISLAND])


def with_boto3(monkeypatch: pytest.MonkeyPatch, stub: object) -> None:
    """*stub* stands in for boto3 (``stub.client(...)`` is what the resolvers call)."""
    with_stubbed_extra(monkeypatch, {"boto3": stub}, [BOTO3_ISLAND])
