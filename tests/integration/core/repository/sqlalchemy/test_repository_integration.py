from __future__ import annotations

import msgspec
from pytest import mark

from loom.core.repository.abc import (
    FilterGroup,
    FilterOp,
    FilterParams,
    FilterSpec,
    PageParams,
    PaginationMode,
    QuerySpec,
    SortSpec,
)
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
    async def test_list_with_query_offset_supports_filters_and_pagination(
        self,
        integration_context: RepositoryIntegrationHarness,
        scenario_catalog_with_price_20: ScenarioDict,
    ) -> None:
        await integration_context.load(scenario_catalog_with_price_20)

        query_page_1 = QuerySpec(
            filters=FilterGroup(filters=(FilterSpec(field="price", op=FilterOp.EQ, value=20.0),)),
            sort=(SortSpec(field="id", direction="ASC"),),
            pagination=PaginationMode.OFFSET,
            limit=1,
            page=1,
        )
        page_1 = await integration_context.product.repository.list_with_query(query_page_1)

        assert page_1.total_count == 2
        assert page_1.page == 1
        assert page_1.limit == 1
        assert page_1.has_next is True
        assert [item.name for item in page_1.items] == ["b"]

        query_page_2 = QuerySpec(
            filters=query_page_1.filters,
            sort=query_page_1.sort,
            pagination=PaginationMode.OFFSET,
            limit=1,
            page=2,
        )
        page_2 = await integration_context.product.repository.list_with_query(query_page_2)

        assert page_2.total_count == 2
        assert page_2.page == 2
        assert page_2.limit == 1
        assert page_2.has_next is False
        assert [item.name for item in page_2.items] == ["c"]

    @mark.asyncio
    async def test_list_with_query_cursor_supports_filters_and_next_cursor(
        self,
        integration_context: RepositoryIntegrationHarness,
        scenario_catalog_with_price_20: ScenarioDict,
    ) -> None:
        await integration_context.load(scenario_catalog_with_price_20)

        first_query = QuerySpec(
            filters=FilterGroup(filters=(FilterSpec(field="price", op=FilterOp.EQ, value=20.0),)),
            sort=(SortSpec(field="id", direction="ASC"),),
            pagination=PaginationMode.CURSOR,
            limit=1,
        )
        first_page = await integration_context.product.repository.list_with_query(first_query)

        assert first_page.has_next is True
        assert first_page.next_cursor is not None
        assert [item.name for item in first_page.items] == ["b"]

        second_query = QuerySpec(
            filters=first_query.filters,
            sort=first_query.sort,
            pagination=PaginationMode.CURSOR,
            limit=1,
            cursor=first_page.next_cursor,
        )
        second_page = await integration_context.product.repository.list_with_query(second_query)

        assert second_page.has_next is False
        assert second_page.next_cursor is None
        assert [item.name for item in second_page.items] == ["c"]

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
