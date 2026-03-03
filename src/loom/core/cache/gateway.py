from __future__ import annotations

import importlib
from collections.abc import Mapping
from typing import Any, TypeVar, cast, overload

import msgspec

T = TypeVar("T")


class CacheGateway:
    """Facade over aiocache with msgpack serialization."""

    def __init__(self, *, alias: str = "default") -> None:
        """Initialise the gateway using the named aiocache alias.

        Args:
            alias: Registered aiocache alias to retrieve the backend from.
        """
        caches = importlib.import_module("aiocache").caches
        self._cache = caches.get(alias)

    @staticmethod
    def configure(raw_config: Mapping[str, Any]) -> None:
        """Apply a configuration mapping to the global aiocache registry.

        Args:
            raw_config: Configuration dict compatible with ``aiocache.caches.set_config``.
        """
        caches = importlib.import_module("aiocache").caches
        caches.set_config(dict(raw_config))

    @overload
    async def get_value(self, key: str, *, type: type[T]) -> T | None: ...

    @overload
    async def get_value(self, key: str, *, type: None = ...) -> Any: ...

    async def get_value(self, key: str, *, type: type[T] | None = None) -> T | Any | None:
        """Retrieve a cached value, optionally converting it to the given type.

        Args:
            key: Cache key.
            type: Optional target type for ``msgspec.convert``.

        Returns:
            The cached value (converted to ``type`` if given) or ``None`` on miss.
        """
        value = await self._cache.get(key)
        if value is None:
            return None
        if type is None:
            return value
        return msgspec.convert(value, type=type)

    async def set_value(self, key: str, value: Any, ttl: int | None = None) -> None:
        """Store a value under the given key.

        Args:
            key: Cache key.
            value: Value to store.
            ttl: Time-to-live in seconds. ``None`` means no expiration.
        """
        await self._cache.set(key, value, ttl=ttl)

    async def multi_get_values(
        self,
        keys: list[str],
        *,
        type: type[T] | None = None,
    ) -> list[T | Any | None]:
        """Retrieve multiple values in a single round-trip.

        Args:
            keys: Cache keys to look up.
            type: Optional target type for ``msgspec.convert`` on each value.

        Returns:
            Values in the same order as ``keys``, with ``None`` for misses.
        """
        values = await self._cache.multi_get(keys)
        if type is not None:
            return [
                msgspec.convert(value, type=type) if value is not None else None for value in values
            ]
        return cast(list[T | Any | None], values)

    async def multi_set_values(
        self,
        pairs: list[tuple[str, Any]],
        ttl: int | None = None,
    ) -> None:
        """Store multiple key-value pairs in a single round-trip.

        Args:
            pairs: List of ``(key, value)`` tuples.
            ttl: Time-to-live in seconds applied to all entries.
        """
        await self._cache.multi_set(pairs, ttl=ttl)

    async def exists(self, key: str) -> bool:
        """Check whether a key exists in the cache.

        Args:
            key: Cache key to check.

        Returns:
            ``True`` if the key is present.
        """
        return bool(await self._cache.exists(key))

    async def delete(self, key: str) -> int:
        """Delete a single key from the cache.

        Args:
            key: Cache key to remove.

        Returns:
            Number of keys actually deleted (0 or 1).
        """
        return int(await self._cache.delete(key))

    async def delete_many(self, keys: list[str]) -> int:
        """Delete multiple keys from the cache.

        Args:
            keys: Cache keys to remove.

        Returns:
            Number of keys actually deleted.
        """
        return int(await self._cache.multi_delete(keys))

    async def clear(self) -> None:
        """Remove all entries from the cache backend."""
        await self._cache.clear()

    async def incr(self, key: str, delta: int = 1) -> int:
        """Increment a numeric value at the given key.

        Args:
            key: Cache key holding an integer value.
            delta: Amount to increment by. Defaults to ``1``.

        Returns:
            The new value after incrementing.
        """
        current = await self.get_value(key, type=int)
        next_value = (0 if current is None else int(current)) + delta
        await self.set_value(key, next_value)
        return next_value

    async def close(self) -> None:
        """Release the underlying cache connection resources."""
        await self._cache.close()
