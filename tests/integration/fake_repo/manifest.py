from __future__ import annotations

from tests.integration.fake_repo.product.category.model import Category
from tests.integration.fake_repo.product.interface import ProductRestInterface
from tests.integration.fake_repo.product.model import Product
from tests.integration.fake_repo.product.relations import ProductCategoryLink
from tests.integration.fake_repo.product.review.model import ProductReview
from tests.integration.fake_repo.product.use_cases import (
    CreateProductUseCase,
    DeleteProductUseCase,
    GetProductUseCase,
    ListProductsUseCase,
    UpdateProductUseCase,
)

MODELS = [Product, Category, ProductReview, ProductCategoryLink]
USE_CASES = [
    CreateProductUseCase,
    GetProductUseCase,
    ListProductsUseCase,
    UpdateProductUseCase,
    DeleteProductUseCase,
]
INTERFACES = [ProductRestInterface]
