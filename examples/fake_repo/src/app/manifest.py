from __future__ import annotations

from app.product.category.model import Category
from app.product.interface import ProductRestInterface
from app.product.model import Product
from app.product.relations import ProductCategoryLink
from app.product.review.model import ProductReview
from app.product.use_cases import (
    CreateProductUseCase,
    DeleteProductUseCase,
    GetProductUseCase,
    ListProductsUseCase,
    UpdateProductUseCase,
)

MODELS = [
    Product,
    Category,
    ProductReview,
    ProductCategoryLink,
]

USE_CASES = [
    CreateProductUseCase,
    ListProductsUseCase,
    GetProductUseCase,
    UpdateProductUseCase,
    DeleteProductUseCase,
]

INTERFACES = [ProductRestInterface]
