from __future__ import annotations

from decimal import Decimal

import pytest

from loom.core.repository.abc import PageParams, QuerySpec
from loom.core.repository.dynamodb.repository import DynamoCapabilityError, RepositoryDynamoDB

from .conftest import FakeTable, Product, ProductCreate, ProductUpdate

pytestmark = pytest.mark.asyncio


def _repo(table: FakeTable) -> RepositoryDynamoDB[Product, int]:
    return RepositoryDynamoDB(table=table, model=Product)


async def test_create_then_get_by_id_roundtrips(fake_table: FakeTable) -> None:
    repo = _repo(fake_table)

    created = await repo.create(ProductCreate(id=1, name="Widget", price=9.5))

    assert created == Product(id=1, name="Widget", price=9.5)
    fetched = await repo.get_by_id(1)
    assert fetched == Product(id=1, name="Widget", price=9.5)


async def test_create_encodes_floats_as_decimal(fake_table: FakeTable) -> None:
    repo = _repo(fake_table)

    await repo.create(ProductCreate(id=1, name="Widget", price=9.5))

    assert fake_table.items[1]["price"] == Decimal("9.5")
    assert isinstance(fake_table.items[1]["price"], Decimal)


async def test_get_by_id_missing_returns_none(fake_table: FakeTable) -> None:
    assert await _repo(fake_table).get_by_id(404) is None


async def test_get_by_primary_key_delegates_to_get_by_id(fake_table: FakeTable) -> None:
    repo = _repo(fake_table)
    await repo.create(ProductCreate(id=7, name="Bolt"))

    assert await repo.get_by("id", 7) == Product(id=7, name="Bolt")


async def test_get_by_non_key_field_raises_capability_error(fake_table: FakeTable) -> None:
    with pytest.raises(DynamoCapabilityError, match="get_by\\('name'\\)"):
        await _repo(fake_table).get_by("name", "Bolt")


async def test_exists_by_primary_key(fake_table: FakeTable) -> None:
    repo = _repo(fake_table)
    await repo.create(ProductCreate(id=3, name="Nut"))

    assert await repo.exists_by("id", 3) is True
    assert await repo.exists_by("id", 999) is False


async def test_exists_by_non_key_field_raises_capability_error(fake_table: FakeTable) -> None:
    with pytest.raises(DynamoCapabilityError, match="exists_by\\('name'\\)"):
        await _repo(fake_table).exists_by("name", "Nut")


async def test_update_merges_fields(fake_table: FakeTable) -> None:
    repo = _repo(fake_table)
    await repo.create(ProductCreate(id=1, name="Widget", price=9.5))

    updated = await repo.update(1, ProductUpdate(name="Gadget"))

    assert updated == Product(id=1, name="Gadget", price=9.5)
    assert await repo.get_by_id(1) == Product(id=1, name="Gadget", price=9.5)


async def test_update_missing_returns_none(fake_table: FakeTable) -> None:
    assert await _repo(fake_table).update(404, ProductUpdate(name="x")) is None


async def test_delete_reports_existence(fake_table: FakeTable) -> None:
    repo = _repo(fake_table)
    await repo.create(ProductCreate(id=1, name="Widget"))

    assert await repo.delete(1) is True
    assert await repo.delete(1) is False
    assert await repo.get_by_id(1) is None


async def test_count_raises_capability_error(fake_table: FakeTable) -> None:
    with pytest.raises(DynamoCapabilityError, match="count"):
        await _repo(fake_table).count()


async def test_list_paginated_raises_capability_error(fake_table: FakeTable) -> None:
    with pytest.raises(DynamoCapabilityError, match="list_paginated"):
        await _repo(fake_table).list_paginated(PageParams(page=1, limit=10))


async def test_list_with_query_raises_capability_error(fake_table: FakeTable) -> None:
    with pytest.raises(DynamoCapabilityError, match="list_with_query"):
        await _repo(fake_table).list_with_query(QuerySpec())
