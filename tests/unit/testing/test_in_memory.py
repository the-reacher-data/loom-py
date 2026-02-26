"""Unit tests for InMemoryRepository."""

from __future__ import annotations

from typing import Any

import msgspec
import pytest

from loom.testing.in_memory import InMemoryRepository

# ---------------------------------------------------------------------------
# Domain fixtures
# ---------------------------------------------------------------------------


class Widget(msgspec.Struct):
    id: int
    name: str


class CreateWidgetCmd(msgspec.Struct):
    name: str


class UpdateWidgetCmd(msgspec.Struct):
    name: str


# ---------------------------------------------------------------------------
# get_by_id
# ---------------------------------------------------------------------------


class TestGetById:
    @pytest.mark.asyncio
    async def test_returns_seeded_entity(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        repo.seed(Widget(id=1, name="A"))
        result = await repo.get_by_id(1)
        assert result == Widget(id=1, name="A")

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        result = await repo.get_by_id(99)
        assert result is None

    @pytest.mark.asyncio
    async def test_profile_param_ignored(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        repo.seed(Widget(id=1, name="A"))
        result = await repo.get_by_id(1, profile="detail")
        assert result == Widget(id=1, name="A")


# ---------------------------------------------------------------------------
# seed
# ---------------------------------------------------------------------------


class TestSeed:
    def test_multiple_entities(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        repo.seed(Widget(id=1, name="A"), Widget(id=2, name="B"))
        assert len(repo._store) == 2

    def test_advances_next_id_past_max(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        repo.seed(Widget(id=5, name="X"))
        assert repo._next_id == 6

    def test_does_not_regress_next_id(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        repo.seed(Widget(id=3, name="A"))
        repo.seed(Widget(id=1, name="B"))
        assert repo._next_id == 4


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------


class TestCreate:
    @pytest.mark.asyncio
    async def test_auto_derives_fields_from_cmd(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        result = await repo.create(CreateWidgetCmd(name="NewWidget"))
        assert result.name == "NewWidget"
        assert result.id == 1

    @pytest.mark.asyncio
    async def test_auto_increments_id(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        w1 = await repo.create(CreateWidgetCmd(name="A"))
        w2 = await repo.create(CreateWidgetCmd(name="B"))
        assert w1.id == 1
        assert w2.id == 2

    @pytest.mark.asyncio
    async def test_stores_created_entity(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        created = await repo.create(CreateWidgetCmd(name="X"))
        stored = await repo.get_by_id(created.id)
        assert stored == created

    @pytest.mark.asyncio
    async def test_id_continues_after_seed(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        repo.seed(Widget(id=10, name="Seed"))
        result = await repo.create(CreateWidgetCmd(name="New"))
        assert result.id == 11

    @pytest.mark.asyncio
    async def test_uses_creator_callable_when_provided(self) -> None:
        def _creator(cmd: Any, next_id: int) -> Widget:
            return Widget(id=next_id * 100, name=cmd.name.upper())

        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget, creator=_creator)
        result = await repo.create(CreateWidgetCmd(name="hello"))
        assert result.id == 100
        assert result.name == "HELLO"


# ---------------------------------------------------------------------------
# update
# ---------------------------------------------------------------------------


class TestUpdate:
    @pytest.mark.asyncio
    async def test_updates_matching_fields(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        repo.seed(Widget(id=1, name="Old"))
        result = await repo.update(1, UpdateWidgetCmd(name="New"))
        assert result is not None
        assert result.name == "New"
        assert result.id == 1

    @pytest.mark.asyncio
    async def test_persists_update(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        repo.seed(Widget(id=1, name="Old"))
        await repo.update(1, UpdateWidgetCmd(name="New"))
        stored = await repo.get_by_id(1)
        assert stored is not None
        assert stored.name == "New"

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        result = await repo.update(99, UpdateWidgetCmd(name="X"))
        assert result is None

    @pytest.mark.asyncio
    async def test_accepts_dict_as_data(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        repo.seed(Widget(id=1, name="Old"))
        result = await repo.update(1, {"name": "DictUpdate"})
        assert result is not None
        assert result.name == "DictUpdate"


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


class TestDelete:
    @pytest.mark.asyncio
    async def test_removes_existing_entity(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        repo.seed(Widget(id=1, name="A"))
        deleted = await repo.delete(1)
        assert deleted is True
        assert await repo.get_by_id(1) is None

    @pytest.mark.asyncio
    async def test_returns_false_when_not_found(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        result = await repo.delete(99)
        assert result is False


# ---------------------------------------------------------------------------
# list_paginated
# ---------------------------------------------------------------------------


class TestListPaginated:
    @pytest.mark.asyncio
    async def test_returns_all_seeded(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        repo.seed(Widget(id=1, name="A"), Widget(id=2, name="B"))
        result = await repo.list_paginated()
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_entities(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        result = await repo.list_paginated()
        assert result == []

    @pytest.mark.asyncio
    async def test_includes_created_entities(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        await repo.create(CreateWidgetCmd(name="X"))
        result = await repo.list_paginated()
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_ignores_extra_kwargs(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        repo.seed(Widget(id=1, name="A"))
        result = await repo.list_paginated(page=1, limit=10, profile="default")
        assert len(result) == 1
