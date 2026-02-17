from __future__ import annotations

from typing import Any

from sqlalchemy import Float, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from loom.core.repository.sqlalchemy.loaders import CountLoader, ExistsLoader, JoinFieldsLoader
from loom.core.repository.sqlalchemy.model import BaseModel
from loom.core.repository.sqlalchemy.projection import Projection
from tests.integration.fake_repo.product.category.model import CategoryModel
from tests.integration.fake_repo.product.relations import ProductCategoryLinkModel
from tests.integration.fake_repo.product.review.model import ProductReviewModel


class ProductModel(BaseModel):
    __tablename__ = "products"

    name: Mapped[str] = mapped_column(String(120), nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    categories: Mapped[list[CategoryModel]] = relationship(
        "CategoryModel",
        secondary=ProductCategoryLinkModel.__table__,
        lazy="noload",
        info={
            "profiles": ("with_details",),
            "depends_on": ("product_category_links:product_id",),
        },
    )
    reviews: Mapped[list[ProductReviewModel]] = relationship(
        "ProductReviewModel",
        back_populates="product",
        lazy="noload",
        info={
            "profiles": ("with_details",),
            "depends_on": ("product_reviews:product_id",),
        },
    )

    has_reviews: Projection[bool] = Projection(
        loader=ExistsLoader(
            table=ProductReviewModel.__table__,
            foreign_key="product_id",
        ),
        profiles=("with_details",),
        depends_on=("product_reviews:product_id",),
        default=False,
    )

    count_reviews: Projection[int] = Projection(
        loader=CountLoader(
            table=ProductReviewModel.__table__,
            foreign_key="product_id",
        ),
        profiles=("with_details",),
        depends_on=("product_reviews:product_id",),
        default=0,
    )

    review_snippets: Projection[list[dict[str, Any]]] = Projection(
        loader=JoinFieldsLoader(
            table=ProductReviewModel.__table__,
            foreign_key="product_id",
            value_columns=("id", "rating", "comment"),
        ),
        profiles=("with_details",),
        depends_on=("product_reviews:product_id",),
        default=[]
    )
