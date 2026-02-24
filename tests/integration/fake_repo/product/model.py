from __future__ import annotations

from typing import Annotated, Any

from loom.core.model import (
    BaseModel,
    Cardinality,
    Field,
    Float,
    Integer,
    OnDelete,
    Projection,
    Relation,
    String,
)
from loom.core.repository.sqlalchemy.loaders import CountLoader, ExistsLoader, JoinFieldsLoader
from tests.integration.fake_repo.product.review.model import ProductReview


class Product(BaseModel):
    __tablename__ = "products"

    id: Annotated[int, Integer, Field(primary_key=True, autoincrement=True)]
    name: Annotated[str, String(120)]
    price: Annotated[float, Float]

    categories: list[dict[str, Any]] = Relation(
        foreign_key="product_id",
        cardinality=Cardinality.MANY_TO_MANY,
        secondary="product_category_links",
        on_delete=OnDelete.CASCADE,
        profiles=("with_details",),
        depends_on=("product_category_links:product_id",),
    )
    reviews: list[dict[str, Any]] = Relation(
        foreign_key="product_id",
        cardinality=Cardinality.ONE_TO_MANY,
        on_delete=OnDelete.CASCADE,
        profiles=("with_details",),
        depends_on=("product_reviews:product_id",),
    )

    has_reviews: bool = Projection(
        loader=ExistsLoader(
            table=ProductReview,
            foreign_key="product_id",
        ),
        profiles=("with_details",),
        depends_on=("product_reviews:product_id",),
        default=False,
    )
    count_reviews: int = Projection(
        loader=CountLoader(
            table=ProductReview,
            foreign_key="product_id",
        ),
        profiles=("with_details",),
        depends_on=("product_reviews:product_id",),
        default=0,
    )
    review_snippets: list[dict[str, Any]] = Projection(
        loader=JoinFieldsLoader(
            table=ProductReview,
            foreign_key="product_id",
            value_columns=("id", "rating", "comment"),
        ),
        profiles=("with_details",),
        depends_on=("product_reviews:product_id",),
        default=[],
    )
