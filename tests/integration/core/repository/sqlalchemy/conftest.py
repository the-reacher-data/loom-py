from __future__ import annotations

from pytest import fixture

from loom.testing import ScenarioDict
from tests.integration.fake_repo.product.category.schemas import CreateCategory
from tests.integration.fake_repo.product.relations import CreateProductCategoryLink
from tests.integration.fake_repo.product.review.schemas import CreateProductReview
from tests.integration.fake_repo.product.schemas import CreateProduct


@fixture
def scenario_one_product() -> ScenarioDict:
    return {
        "product": [
            CreateProduct(name="seed", price=100.0),
        ],
    }


@fixture
def scenario_two_products() -> ScenarioDict:
    return {
        "product": [
            CreateProduct(name="one", price=10.0),
            CreateProduct(name="two", price=20.0),
        ],
    }


@fixture
def scenario_catalog_with_price_20() -> ScenarioDict:
    return {
        "product": [
            CreateProduct(name="a", price=10.0),
            CreateProduct(name="b", price=20.0),
            CreateProduct(name="c", price=20.0),
        ],
    }


@fixture
def scenario_one_product_with_details() -> ScenarioDict:
    return {
        "product": [
            CreateProduct(name="laptop", price=1500.0),
        ],
        "category": [
            CreateCategory(name="electronics"),
        ],
        "category_link": [
            CreateProductCategoryLink(product_id=1, category_id=1),
        ],
        "review": [
            CreateProductReview(product_id=1, rating=5, comment="great"),
            CreateProductReview(product_id=1, rating=4, comment="solid"),
        ],
    }
