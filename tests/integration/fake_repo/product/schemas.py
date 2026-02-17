from __future__ import annotations

import msgspec


class CreateProduct(msgspec.Struct):
    name: str
    price: float


class UpdateProduct(msgspec.Struct, kw_only=True):
    name: str | msgspec.UnsetType = msgspec.UNSET
    price: float | msgspec.UnsetType = msgspec.UNSET
