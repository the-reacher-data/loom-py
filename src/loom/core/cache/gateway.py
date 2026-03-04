from __future__ import annotations

import importlib
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast, overload

import msgspec

from loom.core.cache.serializer import MsgspecSerializer

if TYPE_CHECKING:
    from loom.core.cache.abc.config import CacheConfig

T = TypeVar("T")

_SIMPLE_MEMORY_CACHE = "SimpleMemoryCache"


def _is_raw_backend(cache: Any) -> bool:
    """Return ``True`` when the aiocache backend stores values without serialization.

    A backend is considered "raw" when its configured serializer is NOT
    :class:`~loom.core.cache.serializer.MsgspecSerializer`.  Raw backends
    support native atomic increment operations (Redis ``INCR``,
    ``SimpleMemoryCache.increment``) because there are no encoded bytes to
    corrupt.
    """
    return not isinstance(getattr(cache, "serializer", None), MsgspecSerializer)


class CacheGateway:
    """Facade over aiocache with msgpack serialization for entity data.

    Two distinct usage modes:

    **Data gateway** — configured with
    :class:`~loom.core.cache.serializer.MsgspecSerializer`.  Used by
    :class:`~loom.core.cache.repository.CachedRepository` for entity and list
    storage.  All values are msgpack-encoded on write and decoded on read.

    **Counter gateway** — configured *without* a serializer (raw backend).
    Used by :class:`~loom.core.cache.dependency.GenerationalDependencyResolver`
    for generation counter storage.  Values are stored as plain Python integers,
    enabling atomic native increment on both Redis and ``SimpleMemoryCache``.

    The gateway auto-detects which mode it is in at construction time and routes
    :meth:`incr` accordingly — no manual configuration needed beyond choosing the
    right aiocache alias.

    Args:
        alias: Registered aiocache alias to retrieve the backend from.

    Example::

        data_gateway    = CacheGateway(alias=config.aiocache_alias)
        counter_gateway = CacheGateway(alias=config.effective_counter_alias)
        resolver = GenerationalDependencyResolver(counter_gateway)
    """

    def __init__(self, *, alias: str = "default") -> None:
        """Initialise the gateway using the named aiocache alias.

        Args:
            alias: Registered aiocache alias to retrieve the backend from.
        """
        caches = importlib.import_module("aiocache").caches
        self._cache = caches.get(alias)
        # Raw backends (no MsgspecSerializer) support native atomic increment.
        self._native_counters: bool = _is_raw_backend(self._cache)

    # ------------------------------------------------------------------
    # Configuration helpers
    # ------------------------------------------------------------------

    @staticmethod
    def configure(raw_config: Mapping[str, Any]) -> None:
        """Apply a configuration mapping to the global aiocache registry.

        Args:
            raw_config: Configuration dict compatible with ``aiocache.caches.set_config``.
        """
        caches = importlib.import_module("aiocache").caches
        caches.set_config(dict(raw_config))

    @classmethod
    def apply_config(cls, config: CacheConfig) -> None:
        """Configure aiocache from a :class:`~loom.core.cache.abc.config.CacheConfig`.

        Equivalent to :meth:`configure` but also injects ``config.max_size``
        into every ``aiocache.SimpleMemoryCache`` backend entry that does not
        already declare its own ``max_size``.  Entries for other backend types
        (Redis, Memcached, …) are forwarded unchanged.

        Args:
            config: Resolved cache configuration.

        Example::

            cache_cfg = CacheConfig(
                aiocache_alias="cache",
                counter_alias="counters",
                max_size=1000,
                aiocache_config={
                    "cache":    {"cache": "aiocache.SimpleMemoryCache", ...},
                    "counters": {"cache": "aiocache.SimpleMemoryCache"},
                },
            )
            CacheGateway.apply_config(cache_cfg)
        """
        raw: dict[str, Any] = {}
        for alias, backend_cfg in config.aiocache_config.items():
            if not isinstance(backend_cfg, dict):
                raw[alias] = backend_cfg
                continue
            entry = dict(backend_cfg)
            if config.max_size is not None and _SIMPLE_MEMORY_CACHE in str(entry.get("cache", "")):
                entry.setdefault("max_size", config.max_size)
            raw[alias] = entry
        cls.configure(raw)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

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

    async def exists(self, key: str) -> bool:
        """Check whether a key exists in the cache.

        Args:
            key: Cache key to check.

        Returns:
            ``True`` if the key is present.
        """
        return bool(await self._cache.exists(key))

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    async def set_value(self, key: str, value: Any, ttl: int | None = None) -> None:
        """Store a value under the given key.

        Args:
            key: Cache key.
            value: Value to store.
            ttl: Time-to-live in seconds. ``None`` means no expiration.
        """
        await self._cache.set(key, value, ttl=ttl)

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

    async def incr(self, key: str, delta: int = 1) -> int:
        """Increment a numeric value at the given key.

        Routes to the optimal strategy based on the backend configuration
        detected at construction time:

        - **Raw backend** (no serializer): delegates to the backend's native
          ``increment`` method when available.  On Redis this is an atomic
          ``INCR`` / ``INCRBY`` command; on ``SimpleMemoryCache`` it is
          asyncio-safe.  Falls back to GET+SET for backends that do not expose
          ``increment``.
        - **Serialized backend** (``MsgspecSerializer``): uses a non-atomic
          GET+SET.  Correct for single-process deployments; the asyncio event
          loop does not preempt between the two awaits within a single
          coroutine execution.

        Args:
            key: Cache key holding an integer value.
            delta: Amount to increment by. Defaults to ``1``.

        Returns:
            The new value after incrementing.
        """
        if self._native_counters:
            return await self._native_incr(key, delta)
        return await self._serialized_incr(key, delta)

    async def _native_incr(self, key: str, delta: int) -> int:
        raw = getattr(self._cache, "increment", None)
        if callable(raw):
            fn = cast(Callable[[str, int], Awaitable[Any]], raw)
            return int(await fn(key, delta))
        return await self._serialized_incr(key, delta)

    async def _serialized_incr(self, key: str, delta: int) -> int:
        current = await self.get_value(key, type=int)
        next_value = (0 if current is None else int(current)) + delta
        await self.set_value(key, next_value)
        return next_value

    # ------------------------------------------------------------------
    # Delete / maintenance
    # ------------------------------------------------------------------

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

    async def close(self) -> None:
        """Release the underlying cache connection resources."""
        await self._cache.close()
