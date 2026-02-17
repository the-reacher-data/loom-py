from __future__ import annotations

import msgspec

from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from tests.integration.fake_repo.product.review.model import ProductReviewModel


class ProductReviewRepository(RepositorySQLAlchemy[msgspec.Struct, int]):
    model = ProductReviewModel
