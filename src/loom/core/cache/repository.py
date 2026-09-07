from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from functools import wraps
from typing import Any, Generic, cast

import msgspec

from loom.core.cache._batching import REFILL_BATCH_SIZE, batched
from loom.core.cache.abc.backend import CacheBackend
from loom.core.cache.abc.config import CacheConfig
from loom.core.cache.abc.dependency import BatchFingerprintResolver, DependencyResolver
from loom.core.cache.keys import entity_key, list_index_key, stable_hash
from loom.core.logger import get_logger
from loom.core.model.convert import to_struct
from loom.core.model.enums import Cardinality
from loom.core.model.introspection import (
    get_id_attribute,
    get_projections,
    get_relations,
    list_element_type,
    resolve_type_hints,
)
from loom.core.model.projection import Projection
from loom.core.model.relation import Relation
from loom.core.projection.loaders import resolve_model_reference
from loom.core.repository import FilterParams, MutationEvent, PageParams, PageResult, Repository
from loom.core.repository.abc.query import (
    CursorResult,
    FilterGroup,
    FilterOp,
    FilterSpec,
    PaginationMode,
    QuerySpec,
)
from loom.core.repository.abc.repo_for import BulkCreatable
from loom.core.repository.abc.repository import CreateT, IdT, OutputT, UpdateT


def _infer_otm_cache_dep(model: type, attr_name: str, rel: Relation) -> str | None:
    """Auto-infer ``entity:fk_col`` cache spec for a ONE_TO_MANY relation.

    Resolves the child model type from the field's type annotation and
    combines it with the FK column name from ``rel.foreign_key``.

    Returns ``None`` when cardinality is not ONE_TO_MANY, when the child
    type cannot be resolved, or when it has no ``__tablename__``.
    """
    if rel.cardinality is not Cardinality.ONE_TO_MANY:
        return None
    hints = resolve_type_hints(model)
    child_type = list_element_type(hints.get(attr_name))
    if child_type is None or not hasattr(child_type, "__tablename__"):
        return None
    fk_col = rel.foreign_key.rsplit(".", 1)[-1]
    return f"{child_type.__tablename__}:{fk_col}"


def _infer_projection_cache_dep(model: type, proj: Projection) -> str | None:
    """Auto-infer ``entity:fk_col`` cache spec for a projection loader.

    Looks up the ONE_TO_MANY relation on ``model`` whose child type matches
    the loader's ``model`` attribute, then reuses that relation's FK column.

    Returns ``None`` when the loader has no ``model``, when no matching
    ONE_TO_MANY relation is found, or when types are unresolvable.
    """
    loader_model = resolve_model_reference(getattr(proj.loader, "model", None))
    if loader_model is None or not hasattr(loader_model, "__tablename__"):
        return None
    hints = resolve_type_hints(model)
    for attr_name, rel in get_relations(model).items():
        if rel.cardinality is not Cardinality.ONE_TO_MANY:
            continue
        if list_element_type(hints.get(attr_name)) is loader_model:
            fk_col = rel.foreign_key.rsplit(".", 1)[-1]
            return f"{loader_model.__tablename__}:{fk_col}"
    return None


class _ListIndexPayload(msgspec.Struct):
    ids: list[Any]
    total_count: int


class _QueryIndexPayload(msgspec.Struct):
    ids: list[Any]
    total_count: int | None = None
    next_cursor: str | None = None
    has_next: bool = False


@dataclass(frozen=True, slots=True)
class _DependencySpec:
    entity: str
    fk_field: str


class CachedRepository(
    Repository[OutputT, CreateT, UpdateT, IdT],
    Generic[OutputT, CreateT, UpdateT, IdT],
):
    """Cache-aside wrapper with generational invalidation.

    Attributes the wrapper does not define pass through to the wrapped
    repository, so its capability set (``create_many``, ``count``, custom
    queries) is exactly the wrapped one.

    The cached list path reloads the entities missing from a warm index with a
    single ``id IN (...)`` query, so the wrapped repository should support
    :attr:`~loom.core.repository.abc.query.FilterOp.IN` on the primary key; a
    repository whose ``allowed_filter_fields`` excludes ``id`` is detected and
    served with per-id reads instead.
    """

    def __init__(
        self,
        repository: Repository[OutputT, CreateT, UpdateT, IdT],
        *,
        config: CacheConfig,
        cache: CacheBackend,
        dependency_resolver: DependencyResolver,
    ) -> None:
        self._repository = repository
        self._config = config
        self._cache = cache
        self._resolver = dependency_resolver
        fallback_name = repository.__class__.__name__.lower()
        self._entity_name = getattr(repository, "entity_name", fallback_name)
        self._depends_on = self._parse_dependency_specs(self._collect_dependency_specs(repository))
        self._id_type = self._resolve_id_type(repository)
        self._log = get_logger(__name__).bind(repository=repository.__class__.__name__)

    @property
    def entity_name(self) -> str:
        """Normalized name of the cached entity."""
        return self._entity_name

    async def get_by_id(self, obj_id: IdT, profile: str = "default") -> OutputT | None:
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
        ttl = self._config.ttl_for_single(self.entity_name)
        await self._cache.set_value(key, self._to_builtins(loaded), ttl=ttl)
        return loaded

    async def get_by(
        self,
        field: str,
        value: Any,
        profile: str = "default",
    ) -> OutputT | None:
        """Fetch one entity by arbitrary field.

        This path intentionally delegates to the wrapped repository without
        cache-aside behavior for now. Field-based lookups can target mutable
        columns and the cache invalidation surface is broader than id-based
        access; keeping it uncached preserves correctness while the lookup
        cache policy is designed explicitly.
        """
        return await self._repository.get_by(field, value, profile=profile)

    async def exists_by(self, field: str, value: Any) -> bool:
        """Check existence by arbitrary field.

        Existence checks are delegated directly to the wrapped repository to
        avoid stale negative/positive cache entries on mutable fields.
        """
        return await self._repository.exists_by(field, value)

    async def count(self) -> int:
        """Count every entity, forwarded uncached to the wrapped repository.

        Counts are not cached: a total changes on every write and a stale
        value is worse than the single round trip.
        """
        return await self._repository.count()

    async def list_paginated(
        self,
        page_params: PageParams,
        filter_params: FilterParams | None = None,
        profile: str = "default",
    ) -> PageResult[OutputT]:
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
        entity_ids = None if cached_index is None else self._index_ids(cached_index.ids)
        if cached_index is not None and entity_ids is not None:
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
        page = await self._repository.list_paginated(
            page_params,
            filter_params=filter_params,
            profile=profile,
        )

        ids = [
            cast(IdT, entity_id)
            for item in page.items
            for entity_id in [self._extract_entity_id(item)]
            if entity_id is not None
        ]
        index_to_store = _ListIndexPayload(ids=ids, total_count=page.total_count)
        ttl = self._config.ttl_for_list(self.entity_name)
        await self._cache.set_value(index_key, index_to_store, ttl=ttl)

        await self._cache_entity_batch(page.items, profile=profile)
        return page

    async def list_with_query(
        self,
        query: QuerySpec,
        profile: str = "default",
    ) -> PageResult[OutputT] | CursorResult[OutputT]:
        is_cursor = query.pagination == PaginationMode.CURSOR
        # For cursor pagination, cache only the first page to avoid
        # unbounded key growth and low-hit deep-page caches.
        should_cache = not is_cursor or query.cursor is None
        if not should_cache:
            return await self._repository.list_with_query(query, profile=profile)

        query_payload = self._serialize_query(query)
        query_fingerprint = stable_hash(repr(query_payload))
        tags = self._resolver.list_tags(self.entity_name, query_fingerprint)
        tags.extend(self._list_dependency_tags())
        deps_fingerprint = await self._resolver.fingerprint(tags)
        query_key = (
            f"{self.entity_name}:query:{query_fingerprint}:"
            f"profile={profile}:deps={deps_fingerprint}"
        )

        cached_index = await self._cache.get_value(query_key, type=_QueryIndexPayload)
        entity_ids = None if cached_index is None else self._index_ids(cached_index.ids)
        if cached_index is not None and entity_ids is not None:
            items = await self._load_items_from_index(entity_ids, profile=profile)
            if len(items) == len(entity_ids):
                self._log.debug("CacheHitQuery", key=query_key)
                if is_cursor:
                    return CursorResult(
                        items=tuple(items),
                        next_cursor=cached_index.next_cursor,
                        has_next=cached_index.has_next,
                    )
                return PageResult(
                    items=tuple(items),
                    total_count=0 if cached_index.total_count is None else cached_index.total_count,
                    page=query.page,
                    limit=query.limit,
                    has_next=cached_index.has_next,
                )

        self._log.debug("CacheMissQuery", key=query_key)
        loaded = await self._repository.list_with_query(query, profile=profile)
        ids = [
            cast(IdT, entity_id)
            for item in loaded.items
            for entity_id in [self._extract_entity_id(item)]
            if entity_id is not None
        ]
        ttl = self._config.ttl_for_list(self.entity_name)
        if isinstance(loaded, CursorResult):
            await self._cache.set_value(
                query_key,
                _QueryIndexPayload(
                    ids=ids,
                    next_cursor=loaded.next_cursor,
                    has_next=loaded.has_next,
                ),
                ttl=ttl,
            )
            await self._cache_entity_batch(loaded.items, profile=profile)
            return loaded

        await self._cache.set_value(
            query_key,
            _QueryIndexPayload(
                ids=ids,
                total_count=loaded.total_count,
                has_next=loaded.has_next,
            ),
            ttl=ttl,
        )
        await self._cache_entity_batch(loaded.items, profile=profile)
        return loaded

    async def create(self, data: CreateT) -> OutputT:
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
        await self._resolver.bump_from_events(events)
        post_commit = getattr(self._repository, "on_transaction_committed", None)
        if inspect.iscoroutinefunction(post_commit):
            handler = cast(Callable[[tuple[MutationEvent, ...]], Awaitable[None]], post_commit)
            await handler(events)

    def __getattr__(self, name: str) -> Any:
        """Resolve *name* on the wrapped repository, then adapt it.

        The lookup happens on the wrapped repository first, so a missing
        attribute raises there and ``hasattr(wrapper, "create_many") is
        hasattr(inner, "create_many")`` holds. ``create_many`` is intercepted
        to emit a mutation event after the bulk write; coroutines marked
        with ``@cache_query`` are wrapped with cache-aside behaviour; anything
        else is returned as-is.
        """
        attr = getattr(self._repository, name)
        if name == "create_many":
            return self._create_many
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

    def _extract_entity_id(self, item: Any) -> object | None:
        """Read the primary key of *item*.

        The name ``id`` is assumed here and in the refill filter, which queries
        the missing entities with ``FilterSpec(field="id", ...)``.
        """
        value = getattr(item, "id", None)
        return value

    async def _create_many(self, data: Sequence[msgspec.Struct]) -> tuple[OutputT, ...]:
        if not data:
            return ()
        bulk = cast(BulkCreatable[OutputT], self._repository)
        created = await bulk.create_many(data)
        ids = tuple(
            entity_id
            for item in created
            for entity_id in [self._extract_entity_id(item)]
            if entity_id is not None
        )
        changed_fields = frozenset(key for item in data for key in self._struct_keys(item))
        await self._resolver.bump_from_events(
            (
                MutationEvent(
                    entity=self.entity_name,
                    op="create",
                    ids=ids,
                    changed_fields=changed_fields,
                ),
            )
        )
        return created

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
                ttl = self._config.ttl_for_single(ttl_key or self.entity_name)
            else:
                tags = self._resolver.list_tags(self.entity_name, raw_hash)
                tags.extend(self._list_dependency_tags())
                ttl = self._config.ttl_for_list(ttl_key or self.entity_name)

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
        fingerprints = await self._resolve_fingerprints(tags_by_id)
        entity_keys = [
            entity_key(self.entity_name, eid, profile, fp)
            for eid, fp in zip(ids, fingerprints, strict=True)
        ]
        cached_values = await self._cache.multi_get_values(entity_keys)
        restored = self._restore_cached_page(cached_values)
        missing = [
            (position, ids[position]) for position, item in enumerate(restored) if item is None
        ]
        if not missing:
            return cast(list[OutputT], restored)

        loaded_by_id = await self._fetch_missing_by_ids(
            [missing_id for _, missing_id in missing],
            profile=profile,
        )
        refill: list[tuple[str, OutputT]] = []
        for position, missing_id in missing:
            loaded = loaded_by_id.get(missing_id)
            if loaded is None:
                # The index points at an id the backend no longer returns:
                # let the caller fall back to its own repository query.
                return []
            restored[position] = loaded
            refill.append((entity_keys[position], loaded))

        await self._refill_cache(refill)
        return cast(list[OutputT], restored)

    def _restore_cached_page(self, cached_values: list[Any]) -> list[OutputT | None]:
        """Rebuild each cached payload, leaving ``None`` where the entry is unusable."""
        return [
            None if value is None else self._to_output_from_cache(value) for value in cached_values
        ]

    async def _refill_cache(self, entries: list[tuple[str, OutputT]]) -> None:
        """Write back only the entities that were missing, leaving warm TTLs alone."""
        ttl = self._config.ttl_for_single(self.entity_name)
        pairs = [(key, self._to_builtins(item)) for key, item in entries]
        await self._cache.multi_set_values(pairs, ttl=ttl)

    async def _fetch_missing_by_ids(
        self,
        missing_ids: list[IdT],
        profile: str,
    ) -> dict[object, OutputT]:
        """Reload the entities missing from the cache, keyed by their id."""
        if len(missing_ids) == 1 or not self._supports_id_filter():
            return await self._fetch_missing_one_by_one(missing_ids, profile)
        return await self._fetch_missing_with_query(missing_ids, profile)

    def _supports_id_filter(self) -> bool:
        """Whether the wrapped repository accepts a filter on the primary key.

        A repository may restrict the filterable fields (``allowed_filter_fields``).
        When ``id`` is not among them the batched refill would be rejected, so the
        wrapper degrades to per-id reads rather than failing the page; the
        repository's declared policy is left untouched.
        """
        allowed = getattr(self._repository, "allowed_filter_fields", None)
        return not allowed or "id" in allowed

    async def _fetch_missing_one_by_one(
        self,
        missing_ids: Sequence[IdT],
        profile: str,
    ) -> dict[object, OutputT]:
        loaded: dict[object, OutputT] = {}
        for missing_id in missing_ids:
            item = await self._repository.get_by_id(missing_id, profile=profile)
            if item is not None:
                loaded[missing_id] = item
        return loaded

    async def _fetch_missing_with_query(
        self,
        missing_ids: Sequence[IdT],
        profile: str,
    ) -> dict[object, OutputT]:
        loaded: dict[object, OutputT] = {}
        for batch in batched(missing_ids, REFILL_BATCH_SIZE):
            # Sequential on purpose: the wrapped repository may hold a single
            # session, which is not safe to drive concurrently.
            page = await self._repository.list_with_query(self._id_in_query(batch), profile=profile)
            for item in page.items:
                entity_id = self._extract_entity_id(item)
                if entity_id is not None:
                    loaded[entity_id] = item
        return loaded

    def _id_in_query(self, ids: Sequence[IdT]) -> QuerySpec:
        return QuerySpec(
            filters=FilterGroup(filters=(FilterSpec(field="id", op=FilterOp.IN, value=ids),)),
            limit=len(ids),
        )

    async def _resolve_fingerprints(self, tags_by_id: list[list[str]]) -> list[str]:
        """Resolve one fingerprint per tag group, batched when the resolver allows it."""
        resolver = self._resolver
        if isinstance(resolver, BatchFingerprintResolver):
            return await resolver.fingerprint_many(tags_by_id)
        return list(await asyncio.gather(*(resolver.fingerprint(tags) for tags in tags_by_id)))

    async def _cache_entity_batch(self, items: Sequence[OutputT], profile: str) -> None:
        if not items:
            return
        ttl = self._config.ttl_for_single(self.entity_name)
        entity_ids = [getattr(item, "id", None) for item in items]
        tags_by_id = [
            self._resolver.entity_tags(self.entity_name, entity_id)
            + self._entity_dependency_tags(entity_id)
            for entity_id in entity_ids
        ]
        fingerprints = await self._resolve_fingerprints(tags_by_id)

        pairs: list[tuple[str, Any]] = []
        for item, entity_id, fingerprint in zip(items, entity_ids, fingerprints, strict=True):
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
            model = getattr(self._repository, "model", None)
            if isinstance(model, type) and issubclass(model, msgspec.Struct):
                return cast(OutputT, to_struct(model, payload))
        return cast(OutputT, payload)

    def _resolve_id_type(
        self,
        repository: Repository[OutputT, CreateT, UpdateT, IdT],
    ) -> type | None:
        """Resolve the Python type of the model's primary key, if it is declarable.

        Returns ``None`` for a repository without a model, without a primary key
        or whose key is not a plain class; the cached index is then used as it
        was decoded and the refill runs per id.
        """
        model = getattr(repository, "model", None)
        if model is None:
            return None
        try:
            id_attribute = get_id_attribute(model)
        except (TypeError, ValueError):
            return None
        hint = resolve_type_hints(model).get(id_attribute)
        return hint if isinstance(hint, type) else None

    def _index_ids(self, raw_ids: list[Any]) -> list[IdT] | None:
        """Restore the ids of a cached index to the model's primary-key type.

        The index round-trips through msgpack, which renders a ``datetime``, a
        ``date`` or a ``UUID`` as a string, while the repository binds the
        declared type: SQLAlchemy returns no rows for a ``datetime`` column
        filtered with strings.  Converting here keeps every later use of the id
        — cache key, filter value, identity match — in the declared type.

        Returns:
            The converted ids, or ``None`` when they do not fit the current
            primary-key type, which makes the index unusable.
        """
        if self._id_type is None:
            return cast(list[IdT], raw_ids)
        try:
            return [cast(IdT, msgspec.convert(value, self._id_type)) for value in raw_ids]
        except msgspec.ValidationError:
            self._log.debug("CacheIndexIdTypeMismatch", entity=self.entity_name)
            return None

    def _list_dependency_tags(self) -> list[str]:
        tags: list[str] = []
        for dependency in self._depends_on:
            tags.append(dependency.entity)
            tags.append(f"{dependency.entity}:list")
        return tags

    def _entity_dependency_tags(self, obj_id: object) -> list[str]:
        tags: list[str] = []
        for dependency in self._depends_on:
            tags.append(dependency.entity)
            tags.append(f"{dependency.entity}:list")
            tags.append(f"{dependency.entity}:{dependency.fk_field}:{obj_id}")
        return tags

    def _parse_dependency_specs(self, specs: tuple[str, ...]) -> list[_DependencySpec]:
        deps: list[_DependencySpec] = []
        for spec in specs:
            if ":" not in spec:
                msg = f"Invalid dependency spec '{spec}'. Expected '<entity>:<fk_field>'"
                raise ValueError(msg)
            entity_name, fk_field = spec.split(":", 1)
            entity = entity_name.strip()
            field = fk_field.strip()
            if not entity or not field:
                raise ValueError(f"Invalid dependency spec '{spec}'. Empty entity or fk field")
            deps.append(_DependencySpec(entity=entity, fk_field=field))
        return deps

    def _collect_dependency_specs(
        self,
        repository: Repository[OutputT, CreateT, UpdateT, IdT],
    ) -> tuple[str, ...]:
        specs: list[str] = list(getattr(repository, "depends_on", ()))
        model = getattr(repository, "model", None)
        if model is None:
            return tuple(specs)

        for attr_name, rel in get_relations(model).items():
            if rel.depends_on:
                specs.extend(rel.depends_on)
            else:
                inferred = _infer_otm_cache_dep(model, attr_name, rel)
                if inferred is not None:
                    specs.append(inferred)

        for proj in get_projections(model).values():
            if proj.depends_on:
                specs.extend(proj.depends_on)
            else:
                inferred = _infer_projection_cache_dep(model, proj)
                if inferred is not None:
                    specs.append(inferred)

        return tuple(dict.fromkeys(specs))

    def _to_builtins(self, value: Any) -> Any:
        if isinstance(value, msgspec.Struct):
            return msgspec.to_builtins(value)
        if isinstance(value, list | tuple):
            return [self._to_builtins(item) for item in value]
        if isinstance(value, dict):
            return {str(key): self._to_builtins(item) for key, item in value.items()}
        return value

    def _serialize_filters(self, filter_params: FilterParams | None) -> str:
        if filter_params is None:
            return "{}"
        return repr(self._to_builtins(filter_params.filters))

    def _serialize_query(self, query: QuerySpec) -> dict[str, Any]:
        return {
            "pagination": query.pagination.value,
            "limit": query.limit,
            "page": query.page,
            "cursor": query.cursor,
            "sort": [{"field": sort.field, "direction": sort.direction} for sort in query.sort],
            "filters": self._serialize_filter_group(query.filters),
        }

    def _serialize_filter_group(self, group: FilterGroup | None) -> dict[str, Any] | None:
        if group is None:
            return None
        return {
            "op": group.op,
            "filters": [
                {
                    "field": item.field,
                    "op": item.op.value,
                    "value": self._to_builtins(item.value),
                }
                for item in group.filters
            ],
        }

    def _struct_keys(self, payload: msgspec.Struct) -> list[str]:
        data = msgspec.to_builtins(payload)
        if isinstance(data, dict):
            return list(data.keys())
        return []
