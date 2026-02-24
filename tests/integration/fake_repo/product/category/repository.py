from __future__ import annotations

from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from tests.integration.fake_repo.product.category.model import Category


class CategoryRepository(RepositorySQLAlchemy[Category, int]):
    def __init__(self, session_manager: SessionManager) -> None:
        super().__init__(session_manager=session_manager, model=Category)
