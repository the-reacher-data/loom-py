from __future__ import annotations

import msgspec
from pytest import mark

from loom.core.repository.abc import FilterParams, PageParams
from loom.testing import RepositoryIntegrationHarness, ScenarioDict
from tests.integration.fake_repo.product.model import Product
from tests.integration.fake_repo.product.schemas import CreateProduct, UpdateProduct


class TestRepositorySQLAlchemyIntegration:
    @mark.asyncio
    async def test_crud_flow_with_real_sqlite(
        self,
        integration_context: RepositoryIntegrationHarness,
    ) -> None:
        created = await integration_context.product.repository.create(
            CreateProduct(name="keyboard", price=120.0)
        )
        assert isinstance(created, Product)
        assert created.id == 1

        loaded = await integration_context.product.repository.get_by_id(1)
        assert loaded is not None
        assert isinstance(loaded, Product)
        assert loaded.name == "keyboard"

        updated = await integration_context.product.repository.update(1, UpdateProduct(price=99.9))
        assert updated is not None
        assert float(updated.price) == 99.9

        deleted = await integration_context.product.repository.delete(1)
        assert deleted is True

        missing = await integration_context.product.repository.get_by_id(1)
        assert missing is None

    @mark.asyncio
    async def test_paginated_list_with_filters(
        self,
        integration_context: RepositoryIntegrationHarness,
        scenario_catalog_with_price_20: ScenarioDict,
    ) -> None:
        await integration_context.load(scenario_catalog_with_price_20)

        page = await integration_context.product.repository.list_paginated(
            PageParams(page=1, limit=10),
            FilterParams(filters={"price": 20.0}),
        )

        assert page.total_count == 2
        assert len(page.items) == 2
        assert {item.name for item in page.items} == {"b", "c"}

    @mark.asyncio
    async def test_profile_default_omits_unloaded_fields(
        self,
        integration_context: RepositoryIntegrationHarness,
    ) -> None:
        created = await integration_context.product.repository.create(
            CreateProduct(name="monitor", price=180.0)
        )

        loaded = await integration_context.product.repository.get_by_id(
            created.id, profile="default"
        )
        assert loaded is not None
        data = msgspec.json.decode(msgspec.json.encode(loaded))
        assert "countReviews" not in data
        assert "hasReviews" not in data
        assert "reviewSnippets" not in data

    @mark.asyncio
    async def test_with_details_loads_orm_and_projection_fields(
        self,
        integration_context: RepositoryIntegrationHarness,
        scenario_one_product_with_details: ScenarioDict,
    ) -> None:
        await integration_context.load(scenario_one_product_with_details)
        loaded = await integration_context.product.repository.get_by_id(1, profile="with_details")
        assert loaded is not None
        assert isinstance(loaded, Product)

        categories = loaded.categories
        assert isinstance(categories, list)
        assert {category_item["name"] for category_item in categories} == {"electronics"}

        assert loaded.has_reviews is True
        assert loaded.count_reviews == 2
        assert isinstance(loaded.review_snippets, list)
        assert {snippet["comment"] for snippet in loaded.review_snippets} == {"great", "solid"}
