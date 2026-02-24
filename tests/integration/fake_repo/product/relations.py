from __future__ import annotations

from typing import Annotated

import msgspec

from loom.core.model import BaseModel, Field, Integer, OnDelete
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager


class CreateProductCategoryLink(msgspec.Struct):
    product_id: int
    category_id: int


class ProductCategoryLink(BaseModel):
    __tablename__ = "product_category_links"

    id: Annotated[int, Integer, Field(primary_key=True, autoincrement=True)]
    product_id: Annotated[
        int,
        Integer,
        Field(foreign_key="products.id", on_delete=OnDelete.CASCADE),
    ]
    category_id: Annotated[
        int,
        Integer,
        Field(foreign_key="categories.id", on_delete=OnDelete.CASCADE),
    ]


class ProductCategoryRepository(RepositorySQLAlchemy[ProductCategoryLink, int]):
    context_key = "category_link"

    def __init__(self, session_manager: SessionManager) -> None:
        super().__init__(session_manager=session_manager, model=ProductCategoryLink)
