from __future__ import annotations

import msgspec
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from loom.core.repository.sqlalchemy.model import BaseModel
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy


class CreateProductCategoryLink(msgspec.Struct):
    product_id: int
    category_id: int


class ProductCategoryLinkModel(BaseModel):
    __tablename__ = "product_category_links"

    product_id: Mapped[int] = mapped_column(ForeignKey("products.id", ondelete="CASCADE"), nullable=False)
    category_id: Mapped[int] = mapped_column(ForeignKey("categories.id", ondelete="CASCADE"), nullable=False)


class ProductCategoryRepository(RepositorySQLAlchemy[msgspec.Struct, int]):
    context_key = "category_link"
    model = ProductCategoryLinkModel
