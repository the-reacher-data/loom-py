from __future__ import annotations

from loom.core.model import BaseModel, ColumnField, OnDelete


class ProductReview(BaseModel):
    __tablename__ = "product_reviews"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    product_id: int = ColumnField(foreign_key="products.id", on_delete=OnDelete.CASCADE)
    rating: int = ColumnField()
    comment: str = ColumnField(length=255)
