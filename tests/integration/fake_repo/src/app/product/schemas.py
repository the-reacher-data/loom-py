from __future__ import annotations

from loom.core.command import Command, Patch


class CreateProduct(Command, frozen=True):
    name: str
    price: float


class UpdateProduct(Command, frozen=True):
    name: Patch[str] = None
    price: Patch[float] = None
