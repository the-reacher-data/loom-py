from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from loom.core.repository.sqlalchemy.model import BaseModel

if TYPE_CHECKING:
    from tests.integration.fake_repo.product.model import ProductModel


class ProductReviewModel(BaseModel):
    __tablename__ = "product_reviews"

    product_id: Mapped[int] = mapped_column(ForeignKey("products.id", ondelete="CASCADE"), nullable=False)
    rating: Mapped[int] = mapped_column(Integer, nullable=False)
    comment: Mapped[str] = mapped_column(String(255), nullable=False)
    product: Mapped["ProductModel"] = relationship("ProductModel", back_populates="reviews")
