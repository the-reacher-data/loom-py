from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from loom.core.backend.sqlalchemy import compile_all, get_compiled
from loom.core.command import Command
from loom.core.model import BaseModel, ColumnField
from loom.core.repository.abc import (
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

    async def test_token_issued_for_a_different_sort(
        self, repository: RepositorySQLAlchemy[_Row, int]
    ) -> None:
        token = encode_cursor("sqlalchemy", [3, "a"], 1)

        with pytest.raises(UnsupportedQuery) as excinfo:
            await repository.list_with_query(self._query(token))

        assert "sort" in excinfo.value.reason
