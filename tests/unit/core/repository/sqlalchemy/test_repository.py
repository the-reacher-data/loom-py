from __future__ import annotations

from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import msgspec
import pytest
from pytest import mark

from loom.core.repository.abc import PageParams
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy, with_session_scope
from loom.core.repository.sqlalchemy.session_manager import SessionManager


class TestRepositorySessionScope:
    @mark.asyncio
    async def test_scope_commits_when_session_has_changes(
        self,
        mock_session_manager: Any,
        mock_session: AsyncMock,
        dummy_model: type,
    ) -> None:
        repo: RepositorySQLAlchemy[msgspec.Struct, int] = RepositorySQLAlchemy(
            session_manager=cast(SessionManager, mock_session_manager), model=dummy_model
        )
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
        dummy_model: type,
    ) -> None:
        repo: RepositorySQLAlchemy[msgspec.Struct, int] = RepositorySQLAlchemy(
            session_manager=cast(SessionManager, mock_session_manager), model=dummy_model
        )
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
        dummy_model: type,
    ) -> None:
        repo: RepositorySQLAlchemy[msgspec.Struct, int] = RepositorySQLAlchemy(
            session_manager=cast(SessionManager, mock_session_manager), model=dummy_model
        )

        with pytest.raises(RuntimeError, match="boom"):
            async with repo._session_scope(None):
                raise RuntimeError("boom")

        mock_session.rollback.assert_awaited_once()

    @mark.asyncio
    async def test_scope_reuses_explicit_session_without_commit_or_rollback(
        self,
        mock_session_manager: Any,
        dummy_model: type,
    ) -> None:
        repo: RepositorySQLAlchemy[msgspec.Struct, int] = RepositorySQLAlchemy(
            session_manager=cast(SessionManager, mock_session_manager), model=dummy_model
        )
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
        dummy_model: type,
    ) -> None:
        class _CustomRepo(RepositorySQLAlchemy[msgspec.Struct, int]):
            @with_session_scope
            async def custom_operation(self, session: Any, value: int) -> int:
                await session.flush()
                return value + 1

        repo = _CustomRepo(
            session_manager=cast(SessionManager, mock_session_manager), model=dummy_model
        )

        result = await repo.custom_operation(41)

        assert result == 42
        mock_session.flush.assert_awaited_once()
        mock_session.commit.assert_awaited_once()

    @mark.asyncio
    async def test_decorator_uses_explicit_session_without_auto_commit(
        self,
        mock_session_manager: Any,
        dummy_model: type,
    ) -> None:
        class _CustomRepo(RepositorySQLAlchemy[msgspec.Struct, int]):
            @with_session_scope
            async def custom_operation(self, session: Any, value: int) -> int:
                await session.flush()
                return value + 1

        repo = _CustomRepo(
            session_manager=cast(SessionManager, mock_session_manager), model=dummy_model
        )
        explicit_session = AsyncMock()

        result = await repo.custom_operation(10, session=explicit_session)

        assert result == 11
        explicit_session.flush.assert_awaited_once()
        explicit_session.commit.assert_not_awaited()
        explicit_session.rollback.assert_not_awaited()


class TestRepositoryCoreReadFastPath:
    @mark.asyncio
    async def test_get_by_id_uses_core_row_mapping_path(
        self,
        mock_session_manager: Any,
        mock_session: AsyncMock,
        dummy_model: type,
    ) -> None:
        repo: RepositorySQLAlchemy[msgspec.Struct, int] = RepositorySQLAlchemy(
            session_manager=cast(SessionManager, mock_session_manager), model=dummy_model
        )
        mock_session.get = AsyncMock(side_effect=AssertionError("ORM get should not be used"))

        mappings = Mock()
        mappings.first.return_value = {"id": 7}
        execute_result = Mock()
        execute_result.mappings.return_value = mappings
        mock_session.execute = AsyncMock(return_value=execute_result)

        loaded = await repo.get_by_id(7)

        assert loaded is not None
        assert getattr(loaded, "id", None) == 7
        mock_session.execute.assert_awaited_once()
        mock_session.get.assert_not_awaited()

    @mark.asyncio
    async def test_list_paginated_uses_core_row_mapping_path(
        self,
        mock_session_manager: Any,
        mock_session: AsyncMock,
        dummy_model: type,
    ) -> None:
        repo: RepositorySQLAlchemy[msgspec.Struct, int] = RepositorySQLAlchemy(
            session_manager=cast(SessionManager, mock_session_manager), model=dummy_model
        )

        item_mappings = Mock()
        item_mappings.all.return_value = [{"id": 1}, {"id": 2}]
        item_result = Mock()
        item_result.mappings.return_value = item_mappings

        count_result = Mock()
        count_result.scalar.return_value = 2

        mock_session.execute = AsyncMock(side_effect=[item_result, count_result])

        page = await repo.list_paginated(PageParams(page=1, limit=10))

        assert [getattr(item, "id", None) for item in page.items] == [1, 2]
        assert page.total_count == 2
        assert mock_session.execute.await_count == 2

    def test_can_use_core_read_is_cached_per_profile(
        self,
        mock_session_manager: Any,
        dummy_model: type,
    ) -> None:
        repo: RepositorySQLAlchemy[msgspec.Struct, int] = RepositorySQLAlchemy(
            session_manager=cast(SessionManager, mock_session_manager), model=dummy_model
        )
        options = Mock(return_value=[])
        projections = Mock(return_value={})
        repo._get_profile_options = options  # type: ignore[method-assign]
        repo._projections_for_profile = projections  # type: ignore[method-assign]

        first = repo._can_use_core_read("default")
        second = repo._can_use_core_read("default")

        assert first is True
        assert second is True
        assert options.call_count == 1
        assert projections.call_count == 1
