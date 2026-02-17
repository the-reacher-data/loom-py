from __future__ import annotations

from typing import TYPE_CHECKING

import msgspec

from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from tests.integration.fake_repo.product.model import ProductModel

if TYPE_CHECKING:
    from tests.integration.fake_repo.product.relations import ProductCategoryRepository
    from tests.integration.fake_repo.product.review.repository import ProductReviewRepository


class ProductRepository(RepositorySQLAlchemy[msgspec.Struct, int]):
    model = ProductModel
    review_repo: ProductReviewRepository
    category_link_repo: ProductCategoryRepository
