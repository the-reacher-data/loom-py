"""Decoration contract of ``@cache_call``.

The marker declares; it never wraps. Applying it must leave the very same
function object in the module namespace, carrying a frozen policy a bound
``CachedCalls`` can read later, so a decorated coroutine stays importable and
unit-testable on its own with no cache configured.

Anything that is not a coroutine function is refused at decoration time, by
name: a cached call is awaited once and its single result is stored, which a
``def``, a generator and an async generator cannot honour.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest

from loom.core.cache import cache_call
from loom.core.cache.decorators import declares_cache_call


class TestACoroutineIsMarkedAndReturnedUnchanged:
    """The decorator returns the same object and attaches the policy to it."""

    def test_the_decorated_function_is_the_very_same_object(self) -> None:
        """Decoration must not build a wrapper."""

        async def fetch(query: str) -> str:
            return query

        original = fetch
        decorated = cache_call()(original)

        assert decorated is original

    def test_the_default_policy_carries_the_three_fields(self) -> None:
        """Undecorated defaults: no TTL key, no predicate, version one."""

        @cache_call()
        async def fetch() -> str:
            return "x"

        policy = declares_cache_call(fetch)

        assert policy is not None
        assert policy.ttl_key is None
        assert policy.unless is None
        assert policy.version == 1

    def test_an_explicit_policy_keeps_every_argument(self) -> None:
        """The three declared arguments reach the policy untouched."""

        def empty(result: Any) -> bool:
            return not result

        @cache_call(ttl_key="web_search", unless=empty, version=2)
        async def fetch() -> list[str]:
            return []

        policy = declares_cache_call(fetch)

        assert policy is not None
        assert policy.ttl_key == "web_search"
        assert policy.unless is empty
        assert policy.version == 2

    def test_the_policy_is_frozen(self) -> None:
        """A policy read at wrap time must not be mutable afterwards."""

        @cache_call(ttl_key="web_search")
        async def fetch() -> str:
            return "x"

        policy = declares_cache_call(fetch)

        assert policy is not None
        with pytest.raises(AttributeError):
            policy.ttl_key = "other"  # type: ignore[misc]

    def test_an_unmarked_function_declares_no_policy(self) -> None:
        """``declares_cache_call`` answers ``None`` for a plain coroutine."""

        async def fetch() -> str:
            return "x"

        assert declares_cache_call(fetch) is None


class TestOnlyACoroutineFunctionMayBeMarked:
    """A synchronous, generator or async-generator function is refused by name."""

    def test_a_plain_function_is_refused_naming_it(self) -> None:
        """A ``def`` cannot be awaited, so the decorator raises at import time."""

        def fetch_sync() -> str:
            return "x"

        decorator = cache_call()

        with pytest.raises(TypeError, match="fetch_sync"):
            decorator(fetch_sync)

    def test_a_generator_function_is_refused_naming_it(self) -> None:
        """A generator yields many values; a cached call stores one."""

        def stream() -> Iterator[str]:
            yield "x"

        decorator = cache_call()

        with pytest.raises(TypeError, match="stream"):
            decorator(stream)

    def test_an_async_generator_function_is_refused_naming_it(self) -> None:
        """An async generator is not a coroutine function either."""

        async def astream() -> AsyncIterator[str]:
            yield "x"

        decorator = cache_call()

        with pytest.raises(TypeError, match="astream"):
            decorator(astream)
