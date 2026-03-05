from __future__ import annotations

from typing import Any

from loom.core.model import (
    BaseModel,
    Cardinality,
    ColumnField,
    OnDelete,
    ProjectionField,
    ProjectionSource,
    RelationField,
)
from loom.core.projection.loaders import (
    RelationCountLoader,
    RelationExistsLoader,
    RelationJoinFieldsLoader,
)


class Product(BaseModel):
    __tablename__ = "products"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=120)
    price: float = ColumnField()

    categories: list[dict[str, Any]] = RelationField(
        foreign_key="product_id",
        cardinality=Cardinality.MANY_TO_MANY,
        secondary="product_category_links",
        on_delete=OnDelete.CASCADE,
        profiles=("with_details",),
        depends_on=("product_category_links:product_id",),
    )
    reviews: list[dict[str, Any]] = RelationField(
        foreign_key="product_id",
        cardinality=Cardinality.ONE_TO_MANY,
        on_delete=OnDelete.CASCADE,
        profiles=("with_details",),
        depends_on=("product_reviews:product_id",),
    )

    has_reviews: bool = ProjectionField(
        loader=RelationExistsLoader(
            relation="reviews",
            foreign_key="product_id",
        ),
        source=ProjectionSource.PRELOADED,
        profiles=("with_details",),
        depends_on=("product_reviews:product_id",),
        default=False,
    )
    count_reviews: int = ProjectionField(
        loader=RelationCountLoader(
            relation="reviews",
            foreign_key="product_id",
        ),
        source=ProjectionSource.PRELOADED,
        profiles=("with_details",),
        depends_on=("product_reviews:product_id",),
        default=0,
    )
    review_snippets: list[dict[str, Any]] = ProjectionField(
        loader=RelationJoinFieldsLoader(
            relation="reviews",
            foreign_key="product_id",
            value_columns=("id", "rating", "comment"),
        ),
        source=ProjectionSource.PRELOADED,
        profiles=("with_details",),
        depends_on=("product_reviews:product_id",),
        default=[],
    )
