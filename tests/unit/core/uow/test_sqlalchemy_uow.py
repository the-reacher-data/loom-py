"""Unit tests for SQLAlchemyUnitOfWork using a mocked SessionManager."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

from loom.core.repository.sqlalchemy.transactional import get_active_session
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWork, SQLAlchemyUnitOfWorkFactory
from loom.core.uow.abc import UnitOfWork, UnitOfWorkFactory

# ---------------------------------------------------------------------------
# Fake session / session manager
# ---------------------------------------------------------------------------


def _make_session() -> MagicMock:
    session = MagicMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.close = AsyncMock()
    return session


def _make_session_manager(session: MagicMock) -> MagicMock:
    """Return a mock SessionManager whose .session() yields ``session``."""
    sm = MagicMock()

    @asynccontextmanager
    async def _session_ctx() -> AsyncIterator[MagicMock]:
        yield session

    sm.session = _session_ctx
    return sm


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


def test_sqlalchemy_uow_satisfies_protocol() -> None:
    sm = _make_session_manager(_make_session())
    uow = SQLAlchemyUnitOfWork(sm)
    assert isinstance(uow, UnitOfWork)


def test_sqlalchemy_uow_factory_satisfies_protocol() -> None:
    sm = _make_session_manager(_make_session())
    factory = SQLAlchemyUnitOfWorkFactory(sm)
    assert isinstance(factory, UnitOfWorkFactory)


# ---------------------------------------------------------------------------
# begin / commit / rollback
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_begin_opens_session() -> None:
    session = _make_session()
    sm = _make_session_manager(session)
    uow = SQLAlchemyUnitOfWork(sm)

    await uow.begin()
    assert uow._session is session
    # Cleanup
    await uow._close()


@pytest.mark.asyncio
async def test_commit_calls_session_commit() -> None:
    session = _make_session()
    sm = _make_session_manager(session)
    uow = SQLAlchemyUnitOfWork(sm)

    await uow.begin()
    await uow.commit()
    session.commit.assert_awaited_once()
    await uow._close()


@pytest.mark.asyncio
async def test_rollback_calls_session_rollback() -> None:
    session = _make_session()
    sm = _make_session_manager(session)
    uow = SQLAlchemyUnitOfWork(sm)

    await uow.begin()
    await uow.rollback()
    session.rollback.assert_awaited_once()
    await uow._close()


@pytest.mark.asyncio
async def test_begin_before_commit_raises() -> None:
    session = _make_session()
    sm = _make_session_manager(session)
    uow = SQLAlchemyUnitOfWork(sm)

    with pytest.raises(RuntimeError, match="before begin"):
        await uow.commit()


@pytest.mark.asyncio
async def test_begin_before_rollback_raises() -> None:
    session = _make_session()
    sm = _make_session_manager(session)
    uow = SQLAlchemyUnitOfWork(sm)

    with pytest.raises(RuntimeError, match="before begin"):
        await uow.rollback()


@pytest.mark.asyncio
async def test_begin_twice_raises() -> None:
    session = _make_session()
    sm = _make_session_manager(session)
    uow = SQLAlchemyUnitOfWork(sm)

    await uow.begin()
    with pytest.raises(RuntimeError, match="called twice"):
        await uow.begin()
    await uow._close()


# ---------------------------------------------------------------------------
# Context manager — _active_session ContextVar
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_active_session_set_inside_context_manager() -> None:
    session = _make_session()
    sm = _make_session_manager(session)
    uow = SQLAlchemyUnitOfWork(sm)

    assert get_active_session() is None
    async with uow:
        assert get_active_session() is session


@pytest.mark.asyncio
async def test_active_session_reset_after_context_manager() -> None:
    session = _make_session()
    sm = _make_session_manager(session)
    uow = SQLAlchemyUnitOfWork(sm)

    async with uow:
        pass

    assert get_active_session() is None


@pytest.mark.asyncio
async def test_active_session_reset_after_exception() -> None:
    session = _make_session()
    sm = _make_session_manager(session)
    uow = SQLAlchemyUnitOfWork(sm)

    with pytest.raises(ValueError):
        async with uow:
            raise ValueError("test error")

    assert get_active_session() is None


# ---------------------------------------------------------------------------
# Context manager — commit / rollback behaviour
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_context_manager_commits_on_success() -> None:
    session = _make_session()
    sm = _make_session_manager(session)

    async with SQLAlchemyUnitOfWork(sm):
        pass

    session.commit.assert_awaited_once()
    session.rollback.assert_not_awaited()


@pytest.mark.asyncio
async def test_context_manager_rolls_back_on_exception() -> None:
    session = _make_session()
    sm = _make_session_manager(session)

    with pytest.raises(RuntimeError):
        async with SQLAlchemyUnitOfWork(sm):
            raise RuntimeError("oops")

    session.rollback.assert_awaited_once()
    session.commit.assert_not_awaited()


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def test_factory_create_returns_new_uow() -> None:
    sm = _make_session_manager(_make_session())
    factory = SQLAlchemyUnitOfWorkFactory(sm)
    uow1 = factory.create()
    uow2 = factory.create()
    assert uow1 is not uow2


def test_factory_create_returns_uow_with_same_session_manager() -> None:
    sm = _make_session_manager(_make_session())
    factory = SQLAlchemyUnitOfWorkFactory(sm)
    uow = factory.create()
    assert uow._session_manager is sm
