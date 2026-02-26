from __future__ import annotations

from loom.core.command import Command
from loom.core.model import BaseModel, ColumnField, OnDelete


class CreateProductCategoryLink(Command, frozen=True):
    product_id: int
    category_id: int


class ProductCategoryLink(BaseModel):
    __tablename__ = "product_category_links"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    product_id: int = ColumnField(foreign_key="products.id", on_delete=OnDelete.CASCADE)
    category_id: int = ColumnField(foreign_key="categories.id", on_delete=OnDelete.CASCADE)
