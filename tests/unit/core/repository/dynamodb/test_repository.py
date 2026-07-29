from __future__ import annotations

import pytest

from loom.core.errors import Conflict
from loom.core.repository.abc import PageParams, QuerySpec
from loom.core.repository.dynamodb.repository import DynamoCapabilityError, RepositoryDynamoDB

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


async def test_create_on_existing_id_raises_conflict(fake_client: FakeClient) -> None:
    repo = _repo(fake_client)
    await repo.create(ProductCreate(id=1, name="Widget", price=9.5))

    with pytest.raises(Conflict, match="already exists"):
        await repo.create(ProductCreate(id=1, name="Duplicate"))


async def test_get_by_id_missing_returns_none(fake_client: FakeClient) -> None:
    assert await _repo(fake_client).get_by_id(404) is None


async def test_get_by_primary_key_delegates_to_get_by_id(fake_client: FakeClient) -> None:
    repo = _repo(fake_client)
    await repo.create(ProductCreate(id=7, name="Bolt"))

    assert await repo.get_by("id", 7) == Product(id=7, name="Bolt")


async def test_get_by_non_key_field_raises_capability_error(fake_client: FakeClient) -> None:
    with pytest.raises(DynamoCapabilityError, match="get_by\\('name'\\)"):
        await _repo(fake_client).get_by("name", "Bolt")


async def test_exists_by_primary_key(fake_client: FakeClient) -> None:
    repo = _repo(fake_client)
    await repo.create(ProductCreate(id=3, name="Nut"))

    assert await repo.exists_by("id", 3) is True
    assert await repo.exists_by("id", 999) is False


async def test_exists_by_non_key_field_raises_capability_error(fake_client: FakeClient) -> None:
    with pytest.raises(DynamoCapabilityError, match="exists_by\\('name'\\)"):
        await _repo(fake_client).exists_by("name", "Nut")


async def test_update_merges_fields(fake_client: FakeClient) -> None:
    repo = _repo(fake_client)
    await repo.create(ProductCreate(id=1, name="Widget", price=9.5))

    updated = await repo.update(1, ProductUpdate(name="Gadget"))

    assert updated == Product(id=1, name="Gadget", price=9.5)
    assert await repo.get_by_id(1) == Product(id=1, name="Gadget", price=9.5)


async def test_update_missing_returns_none(fake_client: FakeClient) -> None:
    assert await _repo(fake_client).update(404, ProductUpdate(name="x")) is None


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


async def test_delete_reports_existence(fake_client: FakeClient) -> None:
    repo = _repo(fake_client)
    await repo.create(ProductCreate(id=1, name="Widget"))

    assert await repo.delete(1) is True
    assert await repo.delete(1) is False
    assert await repo.get_by_id(1) is None


async def test_count_raises_capability_error(fake_client: FakeClient) -> None:
    with pytest.raises(DynamoCapabilityError, match="count"):
        await _repo(fake_client).count()


async def test_list_paginated_raises_capability_error(fake_client: FakeClient) -> None:
    with pytest.raises(DynamoCapabilityError, match="list_paginated"):
        await _repo(fake_client).list_paginated(PageParams(page=1, limit=10))


async def test_list_with_query_raises_capability_error(fake_client: FakeClient) -> None:
    with pytest.raises(DynamoCapabilityError, match="list_with_query"):
        await _repo(fake_client).list_with_query(QuerySpec())
