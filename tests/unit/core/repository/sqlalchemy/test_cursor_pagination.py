from __future__ import annotations

import base64
import json
from collections.abc import AsyncIterator
from operator import itemgetter

import pytest

from loom.core.backend.sqlalchemy import compile_all, get_compiled
from loom.core.command import Command
from loom.core.model import BaseModel, ColumnField
from loom.core.repository.abc import (
    CursorResult,
    PaginationMode,
    QuerySpec,
    SortSpec,
    UnsupportedQuery,
)
from loom.core.repository.abc.cursor import encode_cursor
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager


class _Row(BaseModel):
    __tablename__ = "cursor_rows"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    rank: int = ColumnField()
    label: str = ColumnField(length=16)


class _CreateRow(Command, frozen=True):
    rank: int
    label: str


# Duplicate (rank, label) pairs straddle page boundaries at limit=2, so only the id
# tie-breaker keeps the walk exact.
_SEED = [
    (3, "b"),
    (3, "a"),
    (3, "a"),
    (1, "a"),
    (2, "b"),
    (2, "a"),
    (3, "a"),
    (2, "a"),
]


@pytest.fixture
async def session_manager() -> AsyncIterator[SessionManager]:
    compile_all(_Row)
    manager = SessionManager(
        "sqlite+aiosqlite:///:memory:",
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
        pool_recycle=None,
        connect_args={},
    )
    compiled = get_compiled(_Row)
    assert compiled is not None
    async with manager.engine.begin() as connection:
        await connection.run_sync(compiled.metadata.create_all)
    yield manager
    await manager.dispose()


@pytest.fixture
async def repository(session_manager: SessionManager) -> RepositorySQLAlchemy[_Row, int]:
    repo: RepositorySQLAlchemy[_Row, int] = RepositorySQLAlchemy(
        session_manager=session_manager, model=_Row
    )
    await repo.create_many([_CreateRow(rank=rank, label=label) for rank, label in _SEED])
    return repo


def _expected_ids(sort: tuple[SortSpec, ...]) -> list[int]:
    rows = [(index + 1, rank, label) for index, (rank, label) in enumerate(_SEED)]
    ordered = sorted(rows, key=lambda row: row[0])
    for spec in reversed(sort):
        position = {"rank": 1, "label": 2}[spec.field]
        ordered.sort(key=itemgetter(position), reverse=spec.direction == "DESC")
    return [row[0] for row in ordered]


async def _walk(repo: RepositorySQLAlchemy[_Row, int], sort: tuple[SortSpec, ...]) -> list[int]:
    seen: list[int] = []
    cursor: str | None = None
    for _ in range(len(_SEED) + 1):
        result = await repo.list_with_query(
            QuerySpec(sort=sort, pagination=PaginationMode.CURSOR, limit=2, cursor=cursor)
        )
        assert isinstance(result, CursorResult)
        seen.extend(item.id for item in result.items)
        if not result.has_next:
            assert result.next_cursor is None
            return seen
        cursor = result.next_cursor
    raise AssertionError("cursor walk did not terminate")


class TestKeysetWalk:
    async def test_single_key_desc_visits_every_row_once(
        self, repository: RepositorySQLAlchemy[_Row, int]
    ) -> None:
        sort = (SortSpec(field="rank", direction="DESC"),)

        assert await _walk(repository, sort) == _expected_ids(sort)

    async def test_mixed_direction_multi_key_visits_every_row_once(
        self, repository: RepositorySQLAlchemy[_Row, int]
    ) -> None:
        sort = (SortSpec(field="rank", direction="DESC"), SortSpec(field="label", direction="ASC"))

        assert await _walk(repository, sort) == _expected_ids(sort)

    async def test_default_sort_falls_back_to_id(
        self, repository: RepositorySQLAlchemy[_Row, int]
    ) -> None:
        assert await _walk(repository, ()) == list(range(1, len(_SEED) + 1))


class TestRejectedTokens:
    @staticmethod
    def _query(token: str) -> QuerySpec:
        return QuerySpec(
            sort=(SortSpec(field="rank", direction="DESC"),),
            pagination=PaginationMode.CURSOR,
            limit=3,
            cursor=token,
        )

    async def test_token_from_another_backend(
        self, repository: RepositorySQLAlchemy[_Row, int]
    ) -> None:
        with pytest.raises(UnsupportedQuery) as excinfo:
            await repository.list_with_query(self._query(encode_cursor("dynamodb", [3], 1)))

        assert excinfo.value.backend == "sqlalchemy"
        assert excinfo.value.model == "_Row"

    async def test_old_format_token(self, repository: RepositorySQLAlchemy[_Row, int]) -> None:
        token = base64.urlsafe_b64encode(json.dumps({"rank": 3}).encode()).decode()

        with pytest.raises(UnsupportedQuery):
            await repository.list_with_query(self._query(token))

    async def test_garbage_token(self, repository: RepositorySQLAlchemy[_Row, int]) -> None:
        with pytest.raises(UnsupportedQuery):
            await repository.list_with_query(self._query("%%%not-a-token%%%"))

    async def test_token_issued_for_a_different_sort(
        self, repository: RepositorySQLAlchemy[_Row, int]
    ) -> None:
        token = encode_cursor("sqlalchemy", [3, "a"], 1)

        with pytest.raises(UnsupportedQuery) as excinfo:
            await repository.list_with_query(self._query(token))

        assert "sort" in excinfo.value.reason
