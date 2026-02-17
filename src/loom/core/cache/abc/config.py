from __future__ import annotations

from typing import Any

import msgspec


class CacheConfig(msgspec.Struct, kw_only=True):
    """Configuration for cache behaviour including TTLs and aiocache backend settings.

    Attributes:
        enabled: Global toggle for cache operations.
        aiocache_alias: Named alias for the aiocache backend instance.
        aiocache_config: Raw configuration mapping forwarded to aiocache.
        default_ttl: Default time-to-live in seconds for single-entity lookups.
        default_list_ttl: Default time-to-live in seconds for list/index queries.
        ttl: Per-entity TTL overrides keyed by entity name (append ``_list`` for lists).
    """

    enabled: bool = True
    aiocache_alias: str = "default"
    aiocache_config: dict[str, Any] = msgspec.field(default_factory=dict)
    default_ttl: int = 200
    default_list_ttl: int = 120
    ttl: dict[str, int] = msgspec.field(default_factory=dict)

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> "CacheConfig":
        """Create a ``CacheConfig`` from a raw dictionary (e.g. parsed TOML/YAML).

        Args:
            data: Flat or nested mapping with cache configuration values.

        Returns:
            A validated ``CacheConfig`` instance.
        """
        aiocache_cfg = dict(data.get("aiocache", {}))
        return cls(
            enabled=bool(data.get("enabled", True)),
            aiocache_alias=str(data.get("aiocache_alias", "default")),
            aiocache_config=aiocache_cfg,
            default_ttl=int(data.get("default_ttl", 200)),
            default_list_ttl=int(data.get("default_list_ttl", 120)),
            ttl={str(k): int(v) for k, v in dict(data.get("ttl", {})).items()},
        )

    def ttl_for_entity(self, entity: str, *, is_list: bool) -> int:
        """Resolve the effective TTL for an entity, falling back to defaults.

        Args:
            entity: Normalized entity name (e.g. ``"user"``).
            is_list: If ``True``, resolve the list-specific TTL override.

        Returns:
            TTL value in seconds.
        """
        if is_list:
            return self.ttl.get(f"{entity}_list", self.default_list_ttl)
        return self.ttl.get(entity, self.default_ttl)
