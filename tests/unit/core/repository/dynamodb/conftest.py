from __future__ import annotations

from typing import Annotated, Any

from pytest import fixture

from loom.core.model import BaseModel, Field, Float, Integer, String


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


class FakeTable:
    """In-memory stand-in for a boto3 DynamoDB ``Table`` resource.

    Indexes items by a single partition-key attribute and records the raw
    values it is handed so tests can assert Decimal encoding on writes.
    """

    def __init__(self, key_name: str) -> None:
        self._key = key_name
        self.items: dict[Any, dict[str, Any]] = {}

    def get_item(self, Key: dict[str, Any]) -> dict[str, Any]:  # noqa: N803 - boto3 kwarg name
        item = self.items.get(Key[self._key])
        return {"Item": dict(item)} if item is not None else {}

    def put_item(self, Item: dict[str, Any]) -> dict[str, Any]:  # noqa: N803 - boto3 kwarg name
        self.items[Item[self._key]] = dict(Item)
        return {}

    def delete_item(  # noqa: N803 - boto3 kwarg names
        self, Key: dict[str, Any], ReturnValues: str | None = None
    ) -> dict[str, Any]:
        removed = self.items.pop(Key[self._key], None)
        if removed is not None and ReturnValues == "ALL_OLD":
            return {"Attributes": removed}
        return {}


class FakeResource:
    """Stand-in for a boto3 ``dynamodb`` service resource."""

    def __init__(self, tables: dict[str, FakeTable]) -> None:
        self._tables = tables

    def Table(self, name: str) -> FakeTable:  # noqa: N802 - boto3 method name
        return self._tables[name]


@fixture
def product_model() -> type[Product]:
    return Product


@fixture
def fake_table() -> FakeTable:
    return FakeTable(key_name="id")


@fixture
def fake_resource(fake_table: FakeTable) -> FakeResource:
    return FakeResource({"products": fake_table})
