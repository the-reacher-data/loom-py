from __future__ import annotations

from typing import Any, cast
from unittest.mock import AsyncMock

import msgspec
import pytest
from pytest import mark
from sqlalchemy import DateTime  # noqa: F401

from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy, with_session_scope
from loom.core.repository.sqlalchemy.session_manager import SessionManager


class DummyOutput(msgspec.Struct):
    id: int


class _Repository(RepositorySQLAlchemy[DummyOutput, int]):
    model = object


class _CustomRepository(RepositorySQLAlchemy[DummyOutput, int]):
    model = object

    @with_session_scope
    async def custom_operation(self, session: Any, value: int) -> int:
        await session.flush()
        return value + 1


class TestRepositorySessionScope:
    @mark.asyncio
    async def test_scope_commits_when_session_has_changes(
        self,
        mock_session_manager: Any,
        mock_session: AsyncMock,
    ) -> None:
        repo = _Repository(session_manager=cast(SessionManager, mock_session_manager))
        mock_session.new = {"pending"}

        async with repo._session_scope(None) as session:
            assert session is mock_session

        mock_session.commit.assert_awaited_once()
        mock_session.rollback.assert_not_awaited()

    @mark.asyncio
    async def test_scope_commits_when_session_is_read_only(
        self,
        mock_session_manager: Any,
        mock_session: AsyncMock,
    ) -> None:
        repo = _Repository(session_manager=cast(SessionManager, mock_session_manager))
        mock_session.new = set()
        mock_session.dirty = set()
        mock_session.deleted = set()

        async with repo._session_scope(None) as session:
            assert session is mock_session

        mock_session.commit.assert_awaited_once()
        mock_session.rollback.assert_not_awaited()

    @mark.asyncio
    async def test_scope_rolls_back_on_error(
        self,
        mock_session_manager: Any,
        mock_session: AsyncMock,
    ) -> None:
        repo = _Repository(session_manager=cast(SessionManager, mock_session_manager))

        with pytest.raises(RuntimeError, match="boom"):
            async with repo._session_scope(None):
                raise RuntimeError("boom")

        mock_session.rollback.assert_awaited_once()

    @mark.asyncio
    async def test_scope_reuses_explicit_session_without_commit_or_rollback(
        self,
        mock_session_manager: Any,
    ) -> None:
        repo = _Repository(session_manager=cast(SessionManager, mock_session_manager))
        explicit_session = AsyncMock()

        async with repo._session_scope(explicit_session):
            pass

        explicit_session.commit.assert_not_awaited()
        explicit_session.rollback.assert_not_awaited()


class TestWithSessionScopeDecorator:
    @mark.asyncio
    async def test_decorator_opens_scoped_session_and_commits(
        self,
        mock_session_manager: Any,
        mock_session: AsyncMock,
    ) -> None:
        repo = _CustomRepository(session_manager=cast(SessionManager, mock_session_manager))

        result = await repo.custom_operation(41)

        assert result == 42
        mock_session.flush.assert_awaited_once()
        mock_session.commit.assert_awaited_once()

    @mark.asyncio
    async def test_decorator_uses_explicit_session_without_auto_commit(
        self,
        mock_session_manager: Any,
    ) -> None:
        repo = _CustomRepository(session_manager=cast(SessionManager, mock_session_manager))
        explicit_session = AsyncMock()

        result = await repo.custom_operation(10, session=explicit_session)

        assert result == 11
        explicit_session.flush.assert_awaited_once()
        explicit_session.commit.assert_not_awaited()
        explicit_session.rollback.assert_not_awaited()
