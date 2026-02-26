from __future__ import annotations

from typing import Any, TypeVar

import msgspec
from pytest import fixture, mark, raises

from loom.core.cache import (
    CacheConfig,
    CachedRepository,
    GenerationalDependencyResolver,
    cache_query,
    cached,
)
from loom.core.cache.keys import entity_key
from loom.core.model import BaseModel, Cardinality, ColumnField, ProjectionField, RelationField
from loom.core.repository import FilterParams, PageParams, PageResult, Repository
from loom.core.repository.abc.query import (
    CursorResult,
    PaginationMode,
    QuerySpec,
    build_page_result,
)
from loom.core.repository.mutation import MutationEvent

T = TypeVar("T")


class _MemoryCacheBackend:
    def __init__(self) -> None:
        self.data: dict[str, Any] = {}

    async def get(self, key: str, *, type: type[T] | None = None) -> T | Any | None:
        value = self.data.get(key)
        if value is None or type is None:
            return value
        if isinstance(value, dict):
            return type(**value)
        return value

    async def get_value(self, key: str, *, type: type[T] | None = None) -> T | Any | None:
        return await self.get(key, type=type)

    async def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        _ = ttl
        self.data[key] = value

    async def set_value(self, key: str, value: Any, ttl: int | None = None) -> None:
        await self.set(key, value, ttl=ttl)

    async def multi_get(
        self, keys: list[str], *, type: type[T] | None = None
    ) -> list[T | Any | None]:
        result: list[T | Any | None] = []
        for key in keys:
            value = self.data.get(key)
            if value is None or type is None:
                result.append(value)
            elif isinstance(value, dict):
                result.append(type(**value))
            else:
                result.append(value)
        return result

    async def multi_get_values(
        self,
        keys: list[str],
        *,
        type: type[T] | None = None,
    ) -> list[T | Any | None]:
        return await self.multi_get(keys, type=type)

    async def multi_set(self, pairs: list[tuple[str, Any]], ttl: int | None = None) -> None:
        _ = ttl
        for key, value in pairs:
            self.data[key] = value

    async def multi_set_values(self, pairs: list[tuple[str, Any]], ttl: int | None = None) -> None:
        await self.multi_set(pairs, ttl=ttl)

    async def exists(self, key: str) -> bool:
        return key in self.data

    async def delete(self, key: str) -> int:
        if key not in self.data:
            return 0
        del self.data[key]
        return 1

    async def delete_many(self, keys: list[str]) -> int:
        deleted = 0
        for key in keys:
            if key in self.data:
                del self.data[key]
                deleted += 1
        return deleted

    async def incr(self, key: str, delta: int = 1) -> int:
        current = int(self.data.get(key) or 0)
        current += delta
        self.data[key] = current
        return current

    async def close(self) -> None:
        return None


class _FakeSession:
    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _FakeSessionManager:
    async def __aenter__(self) -> _FakeSession:
        return _FakeSession()

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        _ = exc_type
        _ = exc
        _ = tb

    def session(self) -> _FakeSessionManager:
        return self


class _EntityOut(msgspec.Struct):
    id: int
    name: str


class _Create(msgspec.Struct):
    name: str


class _Update(msgspec.Struct, kw_only=True):
    name: str | msgspec.UnsetType = msgspec.UNSET


class _EntityModel:
    __name__ = "EntityModel"


class _ModelWithDependsOn(BaseModel):
    __tablename__ = "products"
    id: int = ColumnField(primary_key=True)
    reviews: list[dict[str, Any]] = RelationField(
        foreign_key="product_id",
        cardinality=Cardinality.ONE_TO_MANY,
        depends_on=("product_reviews:product_id",),
    )
    note_count: int = ProjectionField(
        loader=None,
        depends_on=("product_notes:product_id",),
        default=0,
    )


@cached
class _FakeRepository(Repository[_EntityOut, _Create, _Update, int]):
    model: type[Any] = _EntityModel
    output_type = _EntityOut

    def __init__(self) -> None:
        self.storage: dict[int, _EntityOut] = {}
        self.get_calls = 0
        self.list_calls = 0
        self.query_calls = 0
        self.custom_calls = 0
        self.session_manager = _FakeSessionManager()

    def to_output_from_payload(self, payload: dict[str, Any]) -> _EntityOut:
        return _EntityOut(**payload)

    async def get_by_id(self, obj_id: int, profile: str = "default") -> _EntityOut | None:
        _ = profile
        self.get_calls += 1
        return self.storage.get(obj_id)

    async def list_paginated(
        self,
        page_params: PageParams,
        filter_params: FilterParams | None = None,
        profile: str = "default",
    ) -> PageResult[_EntityOut]:
        _ = filter_params
        _ = profile
        self.list_calls += 1
        values = list(self.storage.values())
        start = page_params.offset
        end = start + page_params.limit
        items = values[start:end]
        return build_page_result(items, len(values), page_params)

    async def list_with_query(
        self,
        query: QuerySpec,
        profile: str = "default",
    ) -> PageResult[_EntityOut] | CursorResult[_EntityOut]:
        _ = profile
        self.query_calls += 1
        if query.pagination == PaginationMode.CURSOR:
            values = tuple(list(self.storage.values())[: query.limit])
            return CursorResult(items=values, next_cursor=None, has_next=False)
        page = PageParams(page=query.page, limit=query.limit)
        return await self.list_paginated(page, profile=profile)

    async def create(self, data: _Create) -> _EntityOut:
        new_id = len(self.storage) + 1
        out = _EntityOut(id=new_id, name=data.name)
        self.storage[new_id] = out
        return out

    async def update(self, obj_id: int, data: _Update) -> _EntityOut | None:
        existing = self.storage.get(obj_id)
        if existing is None:
            return None
        fields = msgspec.to_builtins(data)
        next_name = fields.get("name", existing.name)
        updated = _EntityOut(id=obj_id, name=next_name)
        self.storage[obj_id] = updated
        return updated

    async def delete(self, obj_id: int) -> bool:
        if obj_id not in self.storage:
            return False
        del self.storage[obj_id]
        return True

    @cache_query(scope="list")
    async def find_names(self, prefix: str) -> list[str]:
        self.custom_calls += 1
        return [item.name for item in self.storage.values() if item.name.startswith(prefix)]

    @cache_query(scope="entity")
    async def count_related_notes(self, entity_id: int) -> int:
        self.custom_calls += 1
        return len(self.storage) + entity_id


class _RepoWithModelDependsOn(_FakeRepository):
    model = _ModelWithDependsOn
    depends_on = ("products:id",)


class _RepoWithInvalidDependsOn(_FakeRepository):
    depends_on = ("invalid",)


@fixture
def cache_config() -> CacheConfig:
    return CacheConfig(enabled=True, default_ttl=100, default_list_ttl=50)


@fixture
def wrapped_repository(
    cache_config: CacheConfig,
) -> CachedRepository[_EntityOut, _Create, _Update, int]:
    repository = _FakeRepository()
    cache = _MemoryCacheBackend()
    resolver = GenerationalDependencyResolver(cache)
    return CachedRepository(
        repository, config=cache_config, cache=cache, dependency_resolver=resolver
    )


class TestCachedDecorator:
    def test_marks_repository_class(self) -> None:
        assert getattr(_FakeRepository, "__cache_policy__", False) is True


class TestCachedRepository:
    @mark.asyncio
    async def test_get_by_id_uses_cache_aside(
        self,
        wrapped_repository: CachedRepository[_EntityOut, _Create, _Update, int],
    ) -> None:
        created = await wrapped_repository.create(_Create(name="entity-1"))

        loaded_1 = await wrapped_repository.get_by_id(created.id)
        loaded_2 = await wrapped_repository.get_by_id(created.id)

        assert loaded_1 is not None
        assert loaded_2 is not None
        assert loaded_1.id == created.id
        assert loaded_2.id == created.id
        repo = wrapped_repository._repository
        assert isinstance(repo, _FakeRepository)
        assert repo.get_calls == 1

    @mark.asyncio
    async def test_custom_method_cache_for_developer_function(
        self,
        wrapped_repository: CachedRepository[_EntityOut, _Create, _Update, int],
    ) -> None:
        await wrapped_repository.create(_Create(name="alpha"))
        await wrapped_repository.create(_Create(name="beta"))

        result_1 = await wrapped_repository.find_names("a")
        result_2 = await wrapped_repository.find_names("a")

        assert result_1 == ["alpha"]
        assert result_2 == ["alpha"]
        repo = wrapped_repository._repository
        assert isinstance(repo, _FakeRepository)
        assert repo.custom_calls == 1

    @mark.asyncio
    async def test_delete_invalidates_entity_cache(
        self,
        wrapped_repository: CachedRepository[_EntityOut, _Create, _Update, int],
    ) -> None:
        created = await wrapped_repository.create(_Create(name="entity-1"))
        _ = await wrapped_repository.get_by_id(created.id)
        _ = await wrapped_repository.get_by_id(created.id)

        deleted = await wrapped_repository.delete(created.id)
        loaded_after_delete = await wrapped_repository.get_by_id(created.id)

        assert deleted is True
        assert loaded_after_delete is None
        repo = wrapped_repository._repository
        assert isinstance(repo, _FakeRepository)
        assert repo.get_calls == 2

    @mark.asyncio
    async def test_custom_method_cache_is_invalidated_by_external_event(
        self,
        wrapped_repository: CachedRepository[_EntityOut, _Create, _Update, int],
    ) -> None:
        _ = await wrapped_repository.create(_Create(name="entity-1"))

        first = await wrapped_repository.count_related_notes(1)
        second = await wrapped_repository.count_related_notes(1)
        assert first == second

        await wrapped_repository.on_transaction_committed(
            (
                MutationEvent(
                    entity=wrapped_repository.entity_name,
                    op="create",
                    ids=(1,),
                ),
            )
        )

        third = await wrapped_repository.count_related_notes(1)
        assert third == first
        repo = wrapped_repository._repository
        assert isinstance(repo, _FakeRepository)
        assert repo.custom_calls == 2

    @mark.asyncio
    async def test_list_paginated_refills_missing_entity_from_repository(
        self,
        wrapped_repository: CachedRepository[_EntityOut, _Create, _Update, int],
    ) -> None:
        await wrapped_repository.create(_Create(name="a"))
        await wrapped_repository.create(_Create(name="b"))
        page_params = PageParams(page=1, limit=2)

        first_page = await wrapped_repository.list_paginated(page_params)
        assert len(first_page.items) == 2

        cache_backend = wrapped_repository._cache
        resolver = wrapped_repository._resolver
        first_id = 1
        first_tags = resolver.entity_tags(wrapped_repository.entity_name, first_id)
        first_tags.extend(wrapped_repository._entity_dependency_tags(first_id))
        first_fingerprint = await resolver.fingerprint(first_tags)
        missing_key = entity_key(
            wrapped_repository.entity_name, first_id, "default", first_fingerprint
        )
        await cache_backend.delete(missing_key)

        second_page = await wrapped_repository.list_paginated(page_params)
        assert len(second_page.items) == 2
        assert tuple(item.id for item in second_page.items) == (1, 2)

        repo = wrapped_repository._repository
        assert isinstance(repo, _FakeRepository)
        assert repo.list_calls == 1
        assert repo.get_calls == 1

    @mark.asyncio
    async def test_list_with_query_offset_uses_cache_aside(
        self,
        wrapped_repository: CachedRepository[_EntityOut, _Create, _Update, int],
    ) -> None:
        await wrapped_repository.create(_Create(name="a"))
        await wrapped_repository.create(_Create(name="b"))
        query = QuerySpec(page=1, limit=1, pagination=PaginationMode.OFFSET)

        first = await wrapped_repository.list_with_query(query)
        second = await wrapped_repository.list_with_query(query)

        assert len(first.items) == 1
        assert len(second.items) == 1
        repo = wrapped_repository._repository
        assert isinstance(repo, _FakeRepository)
        assert repo.query_calls == 1

    @mark.asyncio
    async def test_list_with_query_cursor_caches_only_first_page(
        self,
        wrapped_repository: CachedRepository[_EntityOut, _Create, _Update, int],
    ) -> None:
        await wrapped_repository.create(_Create(name="a"))
        await wrapped_repository.create(_Create(name="b"))
        first_page = QuerySpec(limit=1, pagination=PaginationMode.CURSOR)
        next_page = QuerySpec(limit=1, pagination=PaginationMode.CURSOR, cursor="cursor-2")

        _ = await wrapped_repository.list_with_query(first_page)
        _ = await wrapped_repository.list_with_query(first_page)
        _ = await wrapped_repository.list_with_query(next_page)
        _ = await wrapped_repository.list_with_query(next_page)

        repo = wrapped_repository._repository
        assert isinstance(repo, _FakeRepository)
        assert repo.query_calls == 3


class TestDependencySpecs:
    def test_collects_depends_on_from_repository_relation_and_projection(
        self, cache_config: CacheConfig
    ) -> None:
        repository = _RepoWithModelDependsOn()
        cache = _MemoryCacheBackend()
        resolver = GenerationalDependencyResolver(cache)
        wrapped = CachedRepository(
            repository, config=cache_config, cache=cache, dependency_resolver=resolver
        )

        specs = {(item.entity, item.fk_field) for item in wrapped._depends_on}
        assert specs == {
            ("products", "id"),
            ("product_reviews", "product_id"),
            ("product_notes", "product_id"),
        }

    def test_invalid_dependency_spec_fails_fast(self, cache_config: CacheConfig) -> None:
        repository = _RepoWithInvalidDependsOn()
        cache = _MemoryCacheBackend()
        resolver = GenerationalDependencyResolver(cache)

        with raises(ValueError, match="Invalid dependency spec"):
            CachedRepository(
                repository, config=cache_config, cache=cache, dependency_resolver=resolver
            )
