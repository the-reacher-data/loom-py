from __future__ import annotations

from typing import TYPE_CHECKING

from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from tests.integration.fake_repo.product.model import Product

if TYPE_CHECKING:
    from tests.integration.fake_repo.product.category.repository import CategoryRepository
    from tests.integration.fake_repo.product.review.repository import ProductReviewRepository


class ProductRepository(RepositorySQLAlchemy[Product, int]):
    review_repo: ProductReviewRepository
    category_link_repo: CategoryRepository

    def __init__(self, session_manager: SessionManager) -> None:
        super().__init__(session_manager=session_manager, model=Product)
