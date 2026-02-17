from __future__ import annotations

import msgspec

from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from tests.integration.fake_repo.product.category.model import CategoryModel


class CategoryRepository(RepositorySQLAlchemy[msgspec.Struct, int]):
    model = CategoryModel
