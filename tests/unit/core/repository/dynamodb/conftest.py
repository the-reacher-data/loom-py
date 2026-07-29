from __future__ import annotations

from typing import Annotated, Any

from botocore.exceptions import ClientError
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


class FakeClient:
    """In-memory stand-in for a boto3 low-level ``dynamodb`` client.

    Stores items per table in low-level ``AttributeValue`` form (exactly what
    the client API sends and returns), indexed by the single partition-key
    attribute. Honours the ``attribute_not_exists`` / ``attribute_exists``
    conditions used by ``create`` / ``update`` by raising a real
    :class:`botocore.exceptions.ClientError` with the
    ``ConditionalCheckFailedException`` code, so error mapping is exercised
    end-to-end.
    """

    def __init__(self, key_name: str, table_names: tuple[str, ...] = ("products",)) -> None:
        self._key = key_name
        self.items: dict[str, dict[Any, dict[str, Any]]] = {name: {} for name in table_names}

    def get_item(  # noqa: N803 - boto3 kwarg names
        self, *, TableName: str, Key: dict[str, Any]
    ) -> dict[str, Any]:
        item = self.items[TableName].get(self._pk(Key))
        return {"Item": dict(item)} if item is not None else {}

    def put_item(  # noqa: N803 - boto3 kwarg names
        self,
        *,
        TableName: str,
        Item: dict[str, Any],
        ConditionExpression: str | None = None,
        ExpressionAttributeNames: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        table = self.items[TableName]
        pk = self._pk(Item)
        self._check_condition(ConditionExpression, exists=pk in table)
        table[pk] = dict(Item)
        return {}

    def delete_item(  # noqa: N803 - boto3 kwarg names
        self, *, TableName: str, Key: dict[str, Any], ReturnValues: str | None = None
    ) -> dict[str, Any]:
        removed = self.items[TableName].pop(self._pk(Key), None)
        if removed is not None and ReturnValues == "ALL_OLD":
            return {"Attributes": removed}
        return {}

    def _pk(self, item: dict[str, Any]) -> Any:
        """Extract the scalar partition-key value from an AttributeValue dict."""
        return next(iter(item[self._key].values()))

    def _check_condition(self, expression: str | None, *, exists: bool) -> None:
        if expression is None:
            return
        if expression.startswith("attribute_not_exists") and exists:
            raise self._conditional_failure()
        if expression.startswith("attribute_exists") and not exists:
            raise self._conditional_failure()

    @staticmethod
    def _conditional_failure() -> ClientError:
        return ClientError(
            {
                "Error": {
                    "Code": "ConditionalCheckFailedException",
                    "Message": "The conditional request failed",
                }
            },
            "PutItem",
        )


@fixture
def product_model() -> type[Product]:
    return Product


@fixture
def fake_client() -> FakeClient:
    return FakeClient(key_name="id")
