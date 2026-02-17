from __future__ import annotations

from loom.core.cache.abc.backend import CacheBackend
from loom.core.cache.abc.dependency import DependencyResolver
from loom.core.cache.keys import stable_hash
from loom.core.repository.mutation import MutationEvent


class GenerationalDependencyResolver(DependencyResolver):
    """Generational tags with monotonic counters in cache backend."""

    def __init__(self, cache: CacheBackend) -> None:
        """Initialise the resolver with a cache backend for counter storage.

        Args:
            cache: Backend used to persist generation counters.
        """
        self._cache = cache

    def _tag_key(self, tag: str) -> str:
        return f"tag:{tag}"

    async def fingerprint(self, tags: list[str]) -> str:
        """Compute a composite fingerprint from generation counters of all tags.

        Args:
            tags: Dependency tag names.

        Returns:
            A stable hash representing the combined tag generation state.
        """
        if not tags:
            return "0"
        values = await self._cache.multi_get_values([self._tag_key(tag) for tag in tags], type=int)
        parts = [str(0 if value is None else value) for value in values]
        return stable_hash("|".join(parts))

    async def bump_from_events(self, events: tuple[MutationEvent, ...]) -> None:
        """Increment generation counters for all tags affected by mutation events.

        Args:
            events: Mutation events to process.
        """
        bump_keys: set[str] = set()
        for event in events:
            bump_keys.add(self._tag_key(event.entity))
            bump_keys.add(self._tag_key(f"{event.entity}:list"))
            for entity_id in event.ids:
                bump_keys.add(self._tag_key(f"{event.entity}:id:{entity_id}"))
            for tag in event.tags:
                bump_keys.add(self._tag_key(tag))
        for key in bump_keys:
            await self._cache.incr(key, delta=1)

    def entity_tags(self, entity: str, entity_id: object | None) -> list[str]:
        """Return dependency tags for a single entity lookup.

        Args:
            entity: Normalized entity name.
            entity_id: Primary key of the entity, or ``None``.

        Returns:
            List of tag names for this entity.
        """
        tags = [entity]
        if entity_id is not None:
            tags.append(f"{entity}:id:{entity_id}")
        return tags

    def list_tags(self, entity: str, filter_fingerprint: str) -> list[str]:
        """Return dependency tags for a list/index query.

        Args:
            entity: Normalized entity name.
            filter_fingerprint: Hash of the applied filter parameters.

        Returns:
            List of tag names for this list query.
        """
        return [entity, f"{entity}:list", f"{entity}:list:filters:{filter_fingerprint}"]
