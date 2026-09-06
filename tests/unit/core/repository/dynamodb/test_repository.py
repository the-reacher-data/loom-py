from __future__ import annotations

import pytest

from loom.core.repository.abc import UnsupportedQuery
from loom.core.repository.dynamodb.repository import RepositoryDynamoDB

from .conftest import FakeClient, Product, ProductCreate, ProductUpdate

pytestmark = pytest.mark.asyncio


def _repo(client: FakeClient) -> RepositoryDynamoDB[Product, int]:
    return RepositoryDynamoDB(client=client, table_name="products", model=Product)


async def test_create_then_get_by_id_roundtrips(fake_client: FakeClient) -> None:
    repo = _repo(fake_client)

    created = await repo.create(ProductCreate(id=1, name="Widget", price=9.5))

    assert created == Product(id=1, name="Widget", price=9.5)
    fetched = await repo.get_by_id(1)
    assert fetched == Product(id=1, name="Widget", price=9.5)


async def test_create_encodes_floats_as_decimal(fake_client: FakeClient) -> None:
    repo = _repo(fake_client)

    await repo.create(ProductCreate(id=1, name="Widget", price=9.5))

    # Floats cross the wire as low-level ``N`` AttributeValues (never native floats).
    assert fake_client.items["products"]["1"]["price"] == {"N": "9.5"}


async def test_get_by_non_key_field_raises_unsupported_query(fake_client: FakeClient) -> None:
    repo = _repo(fake_client)

    with pytest.raises(UnsupportedQuery, match="get_by\\('name'\\)") as exc_info:
        await repo.get_by("name", "Bolt")

    assert exc_info.value.code == "unsupported_query"
    assert exc_info.value.backend == "dynamodb"
    assert exc_info.value.model == "Product"


async def test_exists_by_primary_key(fake_client: FakeClient) -> None:
    """The contract suite skips exists_by for DynamoDB (no Countable): pin the key path here."""
    repo = _repo(fake_client)
    await repo.create(ProductCreate(id=3, name="Nut"))

    assert await repo.exists_by("id", 3) is True
    assert await repo.exists_by("id", 999) is False


async def test_exists_by_non_key_field_raises_unsupported_query(fake_client: FakeClient) -> None:
    repo = _repo(fake_client)

    with pytest.raises(UnsupportedQuery, match="exists_by\\('name'\\)"):
        await repo.exists_by("name", "Nut")


async def test_update_conditional_failure_returns_none(fake_client: FakeClient) -> None:
    # Item vanishes between the read and the conditional write (concurrent delete);
    # the anti-resurrection condition fails, which maps to "no longer exists" (None).
    repo = _repo(fake_client)
    await repo.create(ProductCreate(id=1, name="Widget", price=9.5))

    original_get = fake_client.get_item

    def _get_then_delete(**kwargs: object) -> dict[str, object]:
        response = original_get(**kwargs)  # type: ignore[arg-type]
        fake_client.items["products"].pop("1", None)
        return response

    fake_client.get_item = _get_then_delete  # type: ignore[method-assign]

    assert await repo.update(1, ProductUpdate(name="Gadget")) is None
