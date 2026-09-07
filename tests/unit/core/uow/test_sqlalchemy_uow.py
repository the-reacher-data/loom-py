"""Unit tests for SQLAlchemyUnitOfWork using a mocked SessionManager."""

from __future__ import annotations

import asyncio
import logging
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


async def _close(uow: SQLAlchemyUnitOfWork) -> None:
    """Close a hand-driven unit of work the documented way: through ``__aexit__``."""
    await uow.__aexit__(RuntimeError, RuntimeError("test cleanup"), None)


class _RecordingSessionManager:
    """Session manager whose context manager counts its exits and may fail to close."""

    def __init__(self, session: MagicMock, exit_error: Exception | None = None) -> None:
        self._session = session
        self._exit_error = exit_error
        self.exits = 0

    def session(self) -> _RecordingSessionManager:
        return self

    async def __aenter__(self) -> MagicMock:
        return self._session

    async def __aexit__(self, *args: object) -> None:
        self.exits += 1
        if self._exit_error is not None:
            raise self._exit_error


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
    await _close(uow)


@pytest.mark.asyncio
async def test_commit_calls_session_commit() -> None:
    session = _make_session()
    sm = _make_session_manager(session)
    uow = SQLAlchemyUnitOfWork(sm)

    await uow.begin()
    await uow.commit()
    session.commit.assert_awaited_once()
    await _close(uow)


@pytest.mark.asyncio
async def test_rollback_calls_session_rollback() -> None:
    session = _make_session()
    sm = _make_session_manager(session)
    uow = SQLAlchemyUnitOfWork(sm)

    await uow.begin()
    await uow.rollback()
    session.rollback.assert_awaited_once()
    await _close(uow)


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
    await _close(uow)


@pytest.mark.asyncio
async def test_a_failure_after_the_session_opened_closes_it_before_propagating(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A4: ``begin`` owns the open session from ``__aenter__`` onwards."""
    session = _make_session()
    sm = _RecordingSessionManager(session)
    uow = SQLAlchemyUnitOfWork(sm)  # type: ignore[arg-type]
    monkeypatch.setattr(
        "loom.core.repository.sqlalchemy.uow.set_active_session",
        MagicMock(side_effect=RuntimeError("context lost")),
    )

    with pytest.raises(RuntimeError, match="context lost"):
        await uow.begin()

    assert sm.exits == 1
    assert get_active_session() is None
    assert uow._session is None


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

    uow = SQLAlchemyUnitOfWork(sm)
    with pytest.raises(RuntimeError):
        async with uow:
            raise RuntimeError("oops")

    session.rollback.assert_awaited_once()
    session.commit.assert_not_awaited()


# ---------------------------------------------------------------------------
# Context manager — commit failure and cancellation (US1 s2)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_commit_failure_rolls_back_then_closes() -> None:
    session = _make_session()
    session.commit = AsyncMock(side_effect=RuntimeError("commit failed"))
    sm = _RecordingSessionManager(session)
    uow = SQLAlchemyUnitOfWork(sm)  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="commit failed"):
        async with uow:
            pass

    session.rollback.assert_awaited_once()
    assert sm.exits == 1
    assert get_active_session() is None


@pytest.mark.asyncio
async def test_close_failure_on_the_rollback_path_keeps_the_business_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    session = _make_session()
    sm = _RecordingSessionManager(session, exit_error=ConnectionError("pool gone"))
    uow = SQLAlchemyUnitOfWork(sm)  # type: ignore[arg-type]

    with (
        caplog.at_level(logging.ERROR, logger=SQLAlchemyUnitOfWork.__module__),
        pytest.raises(RuntimeError, match="business"),
    ):
        async with uow:
            raise RuntimeError("business")

    session.rollback.assert_awaited_once()
    assert sm.exits == 1
    assert "UoWCloseFailed" in caplog.text
    assert get_active_session() is None


@pytest.mark.asyncio
async def test_close_failure_after_commit_is_logged_not_raised(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The write committed: reporting an error would tell the caller it did not."""
    session = _make_session()
    sm = _RecordingSessionManager(session, exit_error=ConnectionError("pool gone"))
    uow = SQLAlchemyUnitOfWork(sm)  # type: ignore[arg-type]

    with caplog.at_level(logging.ERROR):
        async with uow:
            pass

    session.commit.assert_awaited_once()
    assert "UoWCloseFailed" in caplog.text
    assert get_active_session() is None

    session.commit.assert_awaited_once()
    assert get_active_session() is None


@pytest.mark.asyncio
async def test_second_cancellation_during_rollback_still_rolls_back_and_closes() -> None:
    """The driver I/O is shielded; the ContextVar is reset in the caller's context."""
    release = asyncio.Event()
    rolled_back = False

    async def _blocking_rollback() -> None:
        nonlocal rolled_back
        await release.wait()
        rolled_back = True

    session = _make_session()
    session.rollback = _blocking_rollback
    sm = _RecordingSessionManager(session)
    uow = SQLAlchemyUnitOfWork(sm)  # type: ignore[arg-type]

    observed: list[bool] = []

    async def run() -> None:
        try:
            async with uow:
                await asyncio.Event().wait()
        finally:
            observed.append(get_active_session() is None)  # the task's own context

    task = asyncio.create_task(run())
    await asyncio.sleep(0)
    task.cancel()  # cuts the body: the rollback starts and blocks on ``release``
    await asyncio.sleep(0)
    task.cancel()  # arrives while the rollback is in flight
    with pytest.raises(asyncio.CancelledError):
        await task
    assert observed == [True]
    assert not rolled_back

    release.set()
    await asyncio.sleep(0.01)

    assert rolled_back
    assert sm.exits == 1


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
    assert isinstance(uow, SQLAlchemyUnitOfWork)
    assert uow._session_manager is sm
