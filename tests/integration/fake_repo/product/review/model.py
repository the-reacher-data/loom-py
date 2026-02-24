from __future__ import annotations

from typing import Annotated

from loom.core.model import BaseModel, Field, Integer, OnDelete, String


class ProductReview(BaseModel):
    __tablename__ = "product_reviews"

    id: Annotated[int, Integer, Field(primary_key=True, autoincrement=True)]
    product_id: Annotated[
        int,
        Integer,
        Field(foreign_key="products.id", on_delete=OnDelete.CASCADE),
    ]
    rating: Annotated[int, Integer]
    comment: Annotated[str, String(255)]
