from __future__ import annotations

import hashlib


def stable_hash(value: str) -> str:
    """Produce a short, deterministic hash string from the given input.

    Args:
        value: Arbitrary string to hash.

    Returns:
        A 16-character hexadecimal SHA-1 digest prefix.
    """
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:16]


def entity_key(
    entity: str,
    entity_id: object,
    profile: str,
    deps_fingerprint: str,
) -> str:
    """Build the cache key for a single entity lookup.

    Args:
        entity: Normalized entity name.
        entity_id: Primary key of the entity.
        profile: Loading profile name (e.g. ``"default"``).
        deps_fingerprint: Dependency fingerprint for generational invalidation.

    Returns:
        A composite cache key string.
    """
    return f"{entity}:{entity_id}:profile={profile}:deps={deps_fingerprint}"


def list_index_key(
    entity: str,
    filter_fingerprint: str,
    page: int,
    limit: int,
    profile: str,
    deps_fingerprint: str,
) -> str:
    """Build the cache key for a paginated list/index query.

    Args:
        entity: Normalized entity name.
        filter_fingerprint: Hash of the applied filter parameters.
        page: Page number (1-based).
        limit: Maximum number of items per page.
        profile: Loading profile name.
        deps_fingerprint: Dependency fingerprint for generational invalidation.

    Returns:
        A composite cache key string.
    """
    return (
        f"{entity}:list:filters={filter_fingerprint}:page={page}:limit={limit}:"
        f"profile={profile}:deps={deps_fingerprint}"
    )
