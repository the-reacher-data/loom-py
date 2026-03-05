from __future__ import annotations

from typing import Any

import msgspec


class CacheConfig(msgspec.Struct, kw_only=True):
    """Configuration for cache behaviour including TTLs and aiocache backend settings.

    Attributes:
        enabled: Global toggle for cache operations.
        aiocache_alias: Named alias for the aiocache data backend.  The data
            backend **must** be configured with
            :class:`~loom.core.cache.serializer.MsgspecSerializer`.
        counter_alias: Named alias for the counter backend used by
            :class:`~loom.core.cache.dependency.GenerationalDependencyResolver`.
            This backend must have **no serializer** so that native atomic
            increment operations (Redis ``INCR``, ``SimpleMemoryCache.increment``)
            work correctly.  Defaults to ``None``, meaning the same alias as
            ``aiocache_alias`` is used (safe for single-process deployments; uses a
            non-atomic GET+SET fallback automatically).
        aiocache_config: Raw configuration mapping forwarded to ``aiocache``.
            Keyed by alias name.  Use :meth:`apply_config
            <loom.core.cache.CacheGateway.apply_config>` to have ``max_size``
            injected automatically for memory backends.
        default_ttl: Default TTL in seconds for single-entity lookups.
        default_list_ttl: Default TTL in seconds for list / index queries.
        ttl: Per-entity TTL overrides keyed by entity name.  Append ``_list``
            for list overrides (e.g. ``{"user": 300, "user_list": 150}``).
        max_size: Maximum number of entries for ``aiocache.SimpleMemoryCache``
            backends.  Injected automatically when calling
            :meth:`~loom.core.cache.CacheGateway.apply_config`.
            Has no effect on Redis or other non-memory backends.

    Example YAML (Redis + separate counter backend)::

        cache:
          aiocache_alias: cache
          counter_alias: counters
          default_ttl: 300
          default_list_ttl: 120
          max_size: 1000
          ttl:
            user: 600
            user_list: 300
          aiocache:
            cache:
              cache: aiocache.RedisCache
              endpoint: ${oc.env:REDIS_HOST,redis}
              port: 6379
              namespace: myapp
              serializer:
                class: loom.core.cache.serializer.MsgspecSerializer
            counters:
              cache: aiocache.RedisCache
              endpoint: ${oc.env:REDIS_HOST,redis}
              port: 6379
              namespace: myapp_counters
              # no serializer — raw integer storage, atomic Redis INCR

    Example YAML (in-memory for development)::

        cache:
          aiocache_alias: cache
          counter_alias: counters
          max_size: 500
          aiocache:
            cache:
              cache: aiocache.SimpleMemoryCache
              serializer:
                class: loom.core.cache.serializer.MsgspecSerializer
            counters:
              cache: aiocache.SimpleMemoryCache
              # no serializer
    """

    enabled: bool = True
    aiocache_alias: str = "default"
    counter_alias: str | None = None
    aiocache_config: dict[str, Any] = msgspec.field(default_factory=dict)
    default_ttl: int = 200
    default_list_ttl: int = 120
    ttl: dict[str, int] = msgspec.field(default_factory=dict)
    max_size: int | None = None

    @property
    def effective_counter_alias(self) -> str:
        """Return the resolved counter alias, falling back to ``aiocache_alias``."""
        return self.counter_alias or self.aiocache_alias

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> CacheConfig:
        """Create a ``CacheConfig`` from a raw dictionary (e.g. parsed TOML/YAML).

        Args:
            data: Flat or nested mapping with cache configuration values.

        Returns:
            A validated ``CacheConfig`` instance.
        """
        raw_counter_alias = data.get("counter_alias")
        raw_max_size = data.get("max_size")
        return cls(
            enabled=bool(data.get("enabled", True)),
            aiocache_alias=str(data.get("aiocache_alias", "default")),
            counter_alias=str(raw_counter_alias) if raw_counter_alias is not None else None,
            aiocache_config=dict(data.get("aiocache", {})),
            default_ttl=int(data.get("default_ttl", 200)),
            default_list_ttl=int(data.get("default_list_ttl", 120)),
            ttl={str(k): int(v) for k, v in dict(data.get("ttl", {})).items()},
            max_size=int(raw_max_size) if raw_max_size is not None else None,
        )

    def ttl_for_single(self, entity: str) -> int:
        """Resolve the effective TTL for a single-entity lookup.

        Args:
            entity: Normalized entity name (e.g. ``"user"``).

        Returns:
            TTL value in seconds.
        """
        return self.ttl.get(entity, self.default_ttl)

    def ttl_for_list(self, entity: str) -> int:
        """Resolve the effective TTL for a list or index query.

        Args:
            entity: Normalized entity name (e.g. ``"user"``).

        Returns:
            TTL value in seconds.
        """
        return self.ttl.get(f"{entity}_list", self.default_list_ttl)
