from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Sequence
from functools import wraps
from typing import Any, Generic, Mapping, cast

import msgspec

from loom.core.cache.abc.backend import CacheBackend
from loom.core.cache.abc.config import CacheConfig
from loom.core.cache.abc.dependency import DependencyResolver
from loom.core.cache.keys import entity_key, list_index_key, stable_hash
from loom.core.logger import get_logger
from loom.core.repository import FilterParams, MutationEvent, PageParams, PageResult, Repository
from loom.core.repository.abc.repository import CreateT, IdT, OutputT, UpdateT


class _ListIndexPayload(msgspec.Struct):
    ids: list[Any]
    total_count: int


class CachedRepository(
    Repository[OutputT, CreateT, UpdateT, IdT],
    Generic[OutputT, CreateT, UpdateT, IdT],
):
    """Cache-aside wrapper with generational invalidation."""

    def __init__(
        self,
        repository: Repository[OutputT, CreateT, UpdateT, IdT],
        *,
        config: CacheConfig,
        cache: CacheBackend,
        dependency_resolver: DependencyResolver,
    ) -> None:
        """Wrap an existing repository with cache-aside semantics.

        Args:
            repository: The inner repository to delegate persistence to.
            config: Cache configuration (TTLs, toggles).
            cache: Backend used for reading and writing cached values.
            dependency_resolver: Strategy for tag-based generational invalidation.
        """
        self._repository = repository
        self._config = config
        self._cache = cache
        self._resolver = dependency_resolver
        self._entity_name = getattr(repository, "entity_name", repository.__class__.__name__.lower())
        self._depends_on = self._parse_dependency_specs(
            getattr(repository, "depends_on", ()),
        )
        self._log = get_logger(__name__).bind(repository=repository.__class__.__name__)

    @property
    def entity_name(self) -> str:
        """Normalized name of the cached entity."""
        return self._entity_name

    async def get_by_id(self, obj_id: IdT, profile: str = "default") -> OutputT | None:
        """Fetch a single entity by id, returning a cached copy when available.

        Args:
            obj_id: Primary key of the entity.
            profile: Loading profile name for eager-load options.

        Returns:
            The entity output struct, or ``None`` if not found.
        """
        tags = self._resolver.entity_tags(self.entity_name, obj_id)
        tags.extend(self._entity_dependency_tags(obj_id))
        fingerprint = await self._resolver.fingerprint(tags)
        key = entity_key(self.entity_name, obj_id, profile, fingerprint)

        cached_payload = await self._cache.get_value(key)
        if cached_payload is not None:
            self._log.debug("CacheHitEntity", key=key)
            return self._to_output_from_cache(cached_payload)

        self._log.debug("CacheMissEntity", key=key)
        loaded = await self._repository.get_by_id(obj_id, profile=profile)
        if loaded is None:
            return None
        ttl = self._config.ttl_for_entity(self.entity_name, is_list=False)
        await self._cache.set_value(key, self._to_builtins(loaded), ttl=ttl)
        return loaded

    async def list_paginated(
        self,
        page_params: PageParams,
        filter_params: FilterParams | None = None,
        profile: str = "default",
    ) -> PageResult[OutputT]:
        """Fetch a paginated entity list, returning cached results when available.

        Args:
            page_params: Pagination parameters (page number and limit).
            filter_params: Optional filter criteria.
            profile: Loading profile name for eager-load options.

        Returns:
            A ``PageResult`` containing the items and pagination metadata.
        """
        filters_payload = self._serialize_filters(filter_params)
        filter_fingerprint = stable_hash(filters_payload)
        tags = self._resolver.list_tags(self.entity_name, filter_fingerprint)
        tags.extend(self._list_dependency_tags())
        fingerprint = await self._resolver.fingerprint(tags)
        index_key = list_index_key(
            self.entity_name,
            filter_fingerprint,
            page=page_params.page,
            limit=page_params.limit,
            profile=profile,
            deps_fingerprint=fingerprint,
        )
        cached_index = await self._cache.get_value(index_key, type=_ListIndexPayload)
        if cached_index is not None:
            entity_ids = cast(list[IdT], cached_index.ids)
            total_count = cached_index.total_count
            items = await self._load_items_from_index(entity_ids, profile=profile)
            if len(items) == len(entity_ids):
                self._log.debug("CacheHitList", key=index_key)
                return PageResult(
                    items=tuple(items),
                    total_count=total_count,
                    page=page_params.page,
                    limit=page_params.limit,
                    has_next=(page_params.offset + len(items)) < total_count,
                )

        self._log.debug("CacheMissList", key=index_key)
        page = await self._repository.list_paginated(page_params, filter_params=filter_params, profile=profile)

        ids = [cast(IdT, getattr(item, "id")) for item in page.items if hasattr(item, "id")]
        index_to_store = _ListIndexPayload(ids=ids, total_count=page.total_count)
        ttl = self._config.ttl_for_entity(self.entity_name, is_list=True)
        await self._cache.set_value(index_key, index_to_store, ttl=ttl)

        await self._cache_entity_batch(page.items, profile=profile)
        return page

    async def create(self, data: CreateT) -> OutputT:
        """Create a new entity and bump related cache dependency tags.

        Args:
            data: Creation payload struct.

        Returns:
            The newly created entity output struct.
        """
        created = await self._repository.create(data)
        entity_id = getattr(created, "id", None)
        await self._resolver.bump_from_events(
            (
                MutationEvent(
                    entity=self.entity_name,
                    op="create",
                    ids=() if entity_id is None else (entity_id,),
                    changed_fields=frozenset(self._struct_keys(data)),
                ),
            )
        )
        return created

    async def update(self, obj_id: IdT, data: UpdateT) -> OutputT | None:
        """Update an existing entity and bump related cache dependency tags.

        Args:
            obj_id: Primary key of the entity to update.
            data: Partial update payload struct.

        Returns:
            The updated entity output struct, or ``None`` if not found.
        """
        updated = await self._repository.update(obj_id, data)
        if updated is None:
            return None
        await self._resolver.bump_from_events(
            (
                MutationEvent(
                    entity=self.entity_name,
                    op="update",
                    ids=(obj_id,),
                    changed_fields=frozenset(self._struct_keys(data)),
                ),
            )
        )
        return updated

    async def delete(self, obj_id: IdT) -> bool:
        """Delete an entity and bump related cache dependency tags.

        Args:
            obj_id: Primary key of the entity to delete.

        Returns:
            ``True`` if the entity was deleted, ``False`` if not found.
        """
        deleted = await self._repository.delete(obj_id)
        if deleted:
            await self._resolver.bump_from_events(
                (
                    MutationEvent(
                        entity=self.entity_name,
                        op="delete",
                        ids=(obj_id,),
                    ),
                )
            )
        return deleted

    async def on_transaction_committed(self, events: tuple[MutationEvent, ...]) -> None:
        """Handle post-commit hook by bumping dependency tags and forwarding to the inner repository.

        Args:
            events: Mutation events collected during the committed transaction.
        """
        await self._resolver.bump_from_events(events)
        post_commit = getattr(self._repository, "on_transaction_committed", None)
        if callable(post_commit):
            await post_commit(events)

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._repository, name)
        if not callable(attr):
            return attr

        metadata = getattr(attr, "__cache_query__", None)
        if metadata is None:
            return attr
        if inspect.iscoroutinefunction(attr):
            return self._wrap_custom_cached_method(
                name,
                cast(Callable[..., Awaitable[Any]], attr),
                metadata,
            )
        return attr

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _wrap_custom_cached_method(
        self,
        method_name: str,
        method: Callable[..., Awaitable[Any]],
        metadata: dict[str, object],
    ) -> Callable[..., Awaitable[Any]]:
        @wraps(method)
        async def wrapped(*args: Any, **kwargs: Any) -> Any:
            raw = {"args": self._to_builtins(args), "kwargs": self._to_builtins(kwargs)}
            raw_hash = stable_hash(repr(raw))
            scope = cast(str, metadata.get("scope") or "list")
            ttl_key = cast(str | None, metadata.get("ttl_key"))

            if scope == "entity":
                entity_id: object = args[0] if args else raw_hash
                tags = self._resolver.entity_tags(self.entity_name, entity_id)
                tags.extend(self._entity_dependency_tags(entity_id))
                ttl = self._config.ttl_for_entity(ttl_key or self.entity_name, is_list=False)
            else:
                tags = self._resolver.list_tags(self.entity_name, raw_hash)
                tags.extend(self._list_dependency_tags())
                ttl = self._config.ttl_for_entity(ttl_key or self.entity_name, is_list=True)

            fingerprint = await self._resolver.fingerprint(tags)
            key = f"{self.entity_name}:custom:{method_name}:{raw_hash}:deps={fingerprint}"
            cached_payload = await self._cache.get_value(key)
            if cached_payload is not None:
                self._log.debug("CacheHitCustomMethod", key=key, method=method_name)
                return cached_payload

            result = await method(*args, **kwargs)
            await self._cache.set_value(key, self._to_builtins(result), ttl=ttl)
            self._log.debug("CacheMissCustomMethod", key=key, method=method_name)
            return result

        return wrapped

    async def _load_items_from_index(self, ids: list[IdT], profile: str) -> list[OutputT]:
        tags_by_id = [
            self._resolver.entity_tags(self.entity_name, eid) + self._entity_dependency_tags(eid)
            for eid in ids
        ]
        fingerprints = [
            await self._resolver.fingerprint(tags)
            for tags in tags_by_id
        ]
        entity_keys = [
            entity_key(self.entity_name, eid, profile, fp)
            for eid, fp in zip(ids, fingerprints, strict=False)
        ]
        cached_values = await self._cache.multi_get_values(entity_keys)
        items: list[OutputT] = []
        missing_ids: list[IdT] = []
        missing_positions: list[int] = []

        for index, value in enumerate(cached_values):
            if value is None:
                missing_ids.append(ids[index])
                missing_positions.append(index)
                items.append(cast(OutputT, None))
                continue
            restored = self._to_output_from_cache(value)
            if restored is None:
                missing_ids.append(ids[index])
                missing_positions.append(index)
                items.append(cast(OutputT, None))
                continue
            items.append(restored)

        if not missing_ids:
            return items

        for missing_id, position in zip(missing_ids, missing_positions, strict=False):
            loaded = await self._repository.get_by_id(missing_id, profile=profile)
            if loaded is None:
                return []
            items[position] = loaded

        refill_pairs: list[tuple[str, Any]] = []
        ttl = self._config.ttl_for_entity(self.entity_name, is_list=False)
        for obj, ek in zip(items, entity_keys, strict=False):
            refill_pairs.append((ek, self._to_builtins(obj)))
        await self._cache.multi_set_values(refill_pairs, ttl=ttl)

        return items

    async def _cache_entity_batch(self, items: Sequence[OutputT], profile: str) -> None:
        if not items:
            return
        ttl = self._config.ttl_for_entity(self.entity_name, is_list=False)
        pairs: list[tuple[str, Any]] = []
        for item in items:
            entity_id = getattr(item, "id", None)
            tags = self._resolver.entity_tags(self.entity_name, entity_id)
            tags.extend(self._entity_dependency_tags(entity_id))
            fingerprint = await self._resolver.fingerprint(tags)
            key = entity_key(self.entity_name, entity_id, profile, fingerprint)
            pairs.append((key, self._to_builtins(item)))
        await self._cache.multi_set_values(pairs, ttl=ttl)

    def _to_output_from_cache(self, payload: Any) -> OutputT | None:
        if payload is None:
            return None
        if isinstance(payload, msgspec.Struct):
            return cast(OutputT, payload)
        if isinstance(payload, Mapping):
            builder = getattr(self._repository, "to_output_from_payload", None)
            if callable(builder):
                return cast(OutputT, builder(payload))
        return cast(OutputT, payload)

    def _list_dependency_tags(self) -> list[str]:
        tags: list[str] = []
        for dep_entity, _fk_field in self._depends_on:
            tags.append(dep_entity)
            tags.append(f"{dep_entity}:list")
        return tags

    def _entity_dependency_tags(self, obj_id: object) -> list[str]:
        tags: list[str] = []
        for dep_entity, _fk_field in self._depends_on:
            tags.append(dep_entity)
            tags.append(f"{dep_entity}:list")
            tags.append(f"{dep_entity}:id:{obj_id}")
        return tags

    def _parse_dependency_specs(self, specs: tuple[str, ...]) -> list[tuple[str, str]]:
        deps: list[tuple[str, str]] = []
        for spec in specs:
            if ":" not in spec:
                continue
            entity_name, fk_field = spec.split(":", 1)
            entity = entity_name.strip()
            field = fk_field.strip()
            if entity and field:
                deps.append((entity, field))
        return deps

    def _to_builtins(self, value: Any) -> Any:
        if isinstance(value, msgspec.Struct):
            return msgspec.to_builtins(value)
        if isinstance(value, (list, tuple)):
            return [self._to_builtins(item) for item in value]
        if isinstance(value, dict):
            return {str(key): self._to_builtins(item) for key, item in value.items()}
        return value

    def _serialize_filters(self, filter_params: FilterParams | None) -> str:
        if filter_params is None:
            return "{}"
        return repr(self._to_builtins(filter_params.filters))

    def _struct_keys(self, payload: msgspec.Struct) -> list[str]:
        data = msgspec.to_builtins(payload)
        if isinstance(data, dict):
            return list(data.keys())
        return []
