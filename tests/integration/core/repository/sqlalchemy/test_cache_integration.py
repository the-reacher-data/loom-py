from __future__ import annotations

import importlib
from collections.abc import AsyncGenerator
from typing import Any, cast
from unittest.mock import AsyncMock, patch

from pytest import fixture, mark

from loom.core.cache import (
    CacheConfig,
    CachedRepository,
    CacheGateway,
    GenerationalDependencyResolver,
)
from loom.core.repository import Repository
from loom.core.repository.abc import PageParams
from loom.core.repository.mutation import MutationEvent
from loom.testing import RepositoryIntegrationHarness, ScenarioDict
from tests.integration.fake_repo.product.model import Product
from tests.integration.fake_repo.product.review.schemas import CreateProductReview
from tests.integration.fake_repo.product.schemas import CreateProduct, UpdateProduct


@fixture
async def cached_integration_repo(
    integration_context: RepositoryIntegrationHarness,
    cache_backend_kind: str,
) -> AsyncGenerator[CachedRepository[Product, CreateProduct, UpdateProduct, int], None]:
    namespace = "integration_cache"
    aiocache_config: dict[str, object] = {
        "default": {
            "cache": "aiocache.SimpleMemoryCache",
            "serializer": {"class": "loom.core.cache.serializer.MsgspecSerializer"},
            "namespace": "integration_default_cache",
        },
        "test_cache": {
            "cache": "aiocache.SimpleMemoryCache",
            "serializer": {"class": "loom.core.cache.serializer.MsgspecSerializer"},
            "namespace": namespace,
        },
    }

    if cache_backend_kind == "redis-fake":
        fakeredis_async = importlib.import_module("fakeredis.aioredis")
        fake_connection = fakeredis_async.FakeConnection
        aiocache_config["test_cache"] = {
            "cache": "aiocache.RedisCache",
            "serializer": {"class": "loom.core.cache.serializer.MsgspecSerializer"},
            "endpoint": "127.0.0.1",
            "port": 6379,
            "db": 0,
            "namespace": namespace,
            "connection_pool_kwargs": {
                "connection_class": fake_connection,
            },
        }

    CacheGateway.configure(aiocache_config)

    cache_gateway = CacheGateway(alias="test_cache")
    resolver = GenerationalDependencyResolver(cache_gateway)
    cache_config = CacheConfig(
        enabled=True,
        aiocache_alias="test_cache",
        default_ttl=120,
        default_list_ttl=60,
    )
    repository = CachedRepository(
        repository=cast(
            Repository[Product, CreateProduct, UpdateProduct, int],
            integration_context.product.repository,
        ),
        config=cache_config,
        cache=cache_gateway,
        dependency_resolver=resolver,
    )
    try:
        yield repository
    finally:
        await cache_gateway.clear()
        await cache_gateway.close()


@fixture(params=["memory", "redis-fake"])
def cache_backend_kind(request: Any) -> str:
    return cast(str, request.param)


class TestCacheIntegration:
    @staticmethod
    def _spy_base_repo_method(
        cached_repo: CachedRepository[Product, CreateProduct, UpdateProduct, int],
        method_name: str,
    ) -> Any:
        base_repo = cached_repo._repository
        original = getattr(base_repo, method_name)
        mocked = AsyncMock(wraps=original)
        return patch.object(base_repo, method_name, mocked)

    @mark.asyncio
    async def test_get_by_id_cached_and_invalidated_on_update(
        self,
        cached_integration_repo: CachedRepository[Product, CreateProduct, UpdateProduct, int],
        integration_context: RepositoryIntegrationHarness,
        scenario_one_product: ScenarioDict,
    ) -> None:
        # Arrange
        await integration_context.load(scenario_one_product)
        product_id = 1

        # Act / Assert
        with self._spy_base_repo_method(cached_integration_repo, "get_by_id") as get_by_id_spy:
            first = await cached_integration_repo.get_by_id(product_id)
            second = await cached_integration_repo.get_by_id(product_id)

            assert first is not None
            assert second is not None
            assert first.name == "seed"
            assert second.name == "seed"
            assert get_by_id_spy.await_count == 1

            updated = await cached_integration_repo.update(
                product_id, UpdateProduct(name="seed-updated")
            )
            assert updated is not None

            after_update = await cached_integration_repo.get_by_id(product_id)
            assert after_update is not None
            assert after_update.name == "seed-updated"
            assert get_by_id_spy.await_count == 2

    @mark.asyncio
    async def test_list_paginated_cache_hit_and_invalidation_on_create(
        self,
        cached_integration_repo: CachedRepository[Product, CreateProduct, UpdateProduct, int],
        integration_context: RepositoryIntegrationHarness,
        scenario_catalog_with_price_20: ScenarioDict,
    ) -> None:
        # Arrange
        await integration_context.load(scenario_catalog_with_price_20)
        page = PageParams(page=1, limit=2)

        # Act / Assert
        with self._spy_base_repo_method(cached_integration_repo, "list_paginated") as list_spy:
            first = await cached_integration_repo.list_paginated(page)
            second = await cached_integration_repo.list_paginated(page)

            assert first.total_count == 3
            assert second.total_count == 3
            assert list_spy.await_count == 1

            _ = await cached_integration_repo.create(CreateProduct(name="d", price=40.0))
            third = await cached_integration_repo.list_paginated(page)

            assert third.total_count == 4
            assert list_spy.await_count == 2

    @mark.asyncio
    async def test_delete_invalidates_cached_entity(
        self,
        cached_integration_repo: CachedRepository[Product, CreateProduct, UpdateProduct, int],
        integration_context: RepositoryIntegrationHarness,
        scenario_one_product: ScenarioDict,
    ) -> None:
        # Arrange
        await integration_context.load(scenario_one_product)
        product_id = 1

        # Act / Assert
        with self._spy_base_repo_method(cached_integration_repo, "get_by_id") as get_by_id_spy:
            _ = await cached_integration_repo.get_by_id(product_id)
            _ = await cached_integration_repo.get_by_id(product_id)

            deleted = await cached_integration_repo.delete(product_id)
            after_delete = await cached_integration_repo.get_by_id(product_id)

            assert deleted is True
            assert after_delete is None
            assert get_by_id_spy.await_count == 2


class TestRelatedInvalidationIntegration:
    @mark.asyncio
    async def test_with_details_profile_is_invalidated_from_review_repository(
        self,
        cached_integration_repo: CachedRepository[Product, CreateProduct, UpdateProduct, int],
        integration_context: RepositoryIntegrationHarness,
        scenario_one_product: ScenarioDict,
    ) -> None:
        await integration_context.load(scenario_one_product)
        product_id = 1

        first = await cached_integration_repo.get_by_id(product_id, profile="with_details")
        assert first is not None
        assert first.count_reviews == 0
        assert first.has_reviews is False

        await integration_context.review.repository.create(
            CreateProductReview(product_id=product_id, rating=5, comment="awesome")
        )
        await cached_integration_repo.on_transaction_committed(
            (
                MutationEvent(
                    entity="product_reviews",
                    op="create",
                    ids=(1,),
                    tags=frozenset(
                        {
                            f"product_reviews:product_id:{product_id}",
                            "product_reviews",
                            "product_reviews:list",
                        }
                    ),
                ),
            )
        )
        second = await cached_integration_repo.get_by_id(product_id, profile="with_details")
        assert second is not None
        assert second.count_reviews == 1
        assert second.has_reviews is True
        assert {item["comment"] for item in second.review_snippets} == {"awesome"}
