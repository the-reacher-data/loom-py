from __future__ import annotations

from typing import Annotated

import pytest

from loom.core.model import BaseModel, Field, Float, Integer, String

from ._fake import FakeClient as FakeClient


class Product(BaseModel):
    __tablename__ = "products"
    id: Annotated[int, Integer, Field(primary_key=True)]
    name: Annotated[str, String] = ""
    price: Annotated[float, Float] = 0.0


class ProductCreate(BaseModel):
    id: Annotated[int, Integer, Field(primary_key=True)]
    name: Annotated[str, String] = ""
    price: Annotated[float, Float] = 0.0


class ProductUpdate(BaseModel):
    name: Annotated[str, String] = ""


@pytest.fixture
def product_model() -> type[Product]:
    return Product


@pytest.fixture
def fake_client() -> FakeClient:
    return FakeClient(key_name="id")
