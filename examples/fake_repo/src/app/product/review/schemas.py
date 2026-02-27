from __future__ import annotations

from loom.core.command import Command


class CreateProductReview(Command, frozen=True):
    product_id: int
    rating: int
    comment: str
