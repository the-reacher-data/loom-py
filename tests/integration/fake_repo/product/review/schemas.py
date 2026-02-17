from __future__ import annotations

import msgspec


class CreateProductReview(msgspec.Struct):
    product_id: int
    rating: int
    comment: str
