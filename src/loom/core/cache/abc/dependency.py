from __future__ import annotations

from typing import Protocol

from loom.core.repository.mutation import MutationEvent


class DependencyResolver(Protocol):
    """Protocol for cache dependency tracking and invalidation."""

    async def fingerprint(self, tags: list[str]) -> str:
        """Compute a composite fingerprint from the current generation of each tag.

        Args:
            tags: Dependency tag names.

        Returns:
            A stable hash string representing the combined tag state.
        """
        ...

    async def bump_from_events(self, events: tuple[MutationEvent, ...]) -> None:
        """Increment generation counters for all tags affected by mutation events.

        Args:
            events: Mutation events produced within a transaction.
        """
        ...

    def entity_tags(self, entity: str, entity_id: object | None) -> list[str]:
        """Return dependency tags for a single entity lookup.

        Args:
            entity: Normalized entity name.
            entity_id: Primary key of the entity, or ``None``.

        Returns:
            List of tag names that should be tracked for this entity.
        """
        ...

    def list_tags(self, entity: str, filter_fingerprint: str) -> list[str]:
        """Return dependency tags for a list/index query.

        Args:
            entity: Normalized entity name.
            filter_fingerprint: Hash of the applied filter parameters.

        Returns:
            List of tag names that should be tracked for this list query.
        """
        ...
