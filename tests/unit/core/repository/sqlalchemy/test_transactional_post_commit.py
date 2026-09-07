"""``@transactional`` runs its post-commit hooks through the shared channel."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock

import pytest

from loom.core.engine.post_commit import (
    PostCommitChannel,
    PostCommitError,
    active_channel,
    bind_channel,
    reset_channel,
)
from loom.core.repository.mutation import MutationEvent
from loom.core.repository.sqlalchemy.transactional import (
    get_active_session,
    transactional,
)


class _RecordingSessionManager:
    """Session manager that records when the session scope closes."""

    def __init__(self, session: AsyncMock, log: list[str]) -> None:
        self._session = session
        self._log = log

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncMock]:
        try:
            yield self._session
        finally:
            self._log.append("session_closed")


class _Dependency:
    def __init__(self, log: list[str], name: str) -> None:
        self._log = log
        self._name = name
        self.events: tuple[MutationEvent, ...] | None = None

    async def on_transaction_committed(self, events: tuple[MutationEvent, ...]) -> None:
        self.events = events
        self._log.append(self._name)


class _Service:
    def __init__(self, session_manager: Any, log: list[str]) -> None:
        self.session_manager = session_manager
        self.log = log
        self.dependency_a = _Dependency(log, "dep_a")
        self.dependency_b = _Dependency(log, "dep_b")
        self.owner_error: Exception | None = None

    async def on_transaction_committed(self, events: tuple[MutationEvent, ...]) -> None:
        self.log.append("owner")
        if self.owner_error is not None:
            raise self.owner_error

    @transactional
    async def execute(self) -> str:
        self.log.append("body")
        return "ok"

    @transactional
    async def fail(self) -> None:
        raise RuntimeError("boom")


class _Outer:
    def __init__(self, session_manager: Any, log: list[str]) -> None:
        self.session_manager = session_manager
        self.log = log
        self.inner = _Service(session_manager, log)

    async def on_transaction_committed(self, events: tuple[MutationEvent, ...]) -> None:
        self.log.append("outer")

    @transactional
    async def execute(self) -> str:
        return await self.inner.execute()


@pytest.fixture
def log() -> list[str]:
    return []


@pytest.fixture
def recording_session(log: list[str]) -> AsyncMock:
    session = AsyncMock()

    async def _commit() -> None:
        log.append("commit")

    session.commit.side_effect = _commit
    return session


@pytest.fixture
def service(recording_session: AsyncMock, log: list[str]) -> _Service:
    return _Service(_RecordingSessionManager(recording_session, log), log)


class TestOwnedChannel:
    async def test_hooks_run_in_order_after_commit_and_session_close(
        self, service: _Service, log: list[str]
    ) -> None:
        result = await service.execute()

        assert result == "ok"
        assert log == ["body", "commit", "session_closed", "owner", "dep_a", "dep_b"]

    async def test_hooks_see_no_active_session(self, service: _Service) -> None:
        seen: list[Any] = []

        async def _hook(events: tuple[MutationEvent, ...]) -> None:
            seen.append(get_active_session())

        service.dependency_a.on_transaction_committed = _hook  # type: ignore[method-assign]

        await service.execute()

        assert seen == [None]

    async def test_rollback_discards_hooks(
        self, service: _Service, recording_session: AsyncMock, log: list[str]
    ) -> None:
        with pytest.raises(RuntimeError, match="boom"):
            await service.fail()

        recording_session.rollback.assert_awaited_once()
        assert log == ["session_closed"]
        assert active_channel() is None

    async def test_failing_hook_raises_post_commit_error_after_the_others_ran(
        self, service: _Service, log: list[str]
    ) -> None:
        service.owner_error = ValueError("hook failed")

        with pytest.raises(PostCommitError) as excinfo:
            await service.execute()

        assert excinfo.value.committed is True
        assert excinfo.value.failures == (service.owner_error,)
        assert log[-3:] == ["owner", "dep_a", "dep_b"]

    async def test_channel_unbound_after_success(self, service: _Service) -> None:
        await service.execute()

        assert active_channel() is None

    async def test_nested_call_under_active_session_enqueues_nothing_extra(
        self, recording_session: AsyncMock, log: list[str]
    ) -> None:
        outer = _Outer(_RecordingSessionManager(recording_session, log), log)

        await outer.execute()

        recording_session.commit.assert_awaited_once()
        # The owner walk is one level deep: ``inner`` is a dependency of
        # ``outer`` and runs once; it did not bind a channel of its own.
        assert log == ["body", "commit", "session_closed", "outer", "owner"]


class TestBoundChannel:
    async def test_owns_its_channel_even_when_one_is_bound(
        self, service: _Service, log: list[str]
    ) -> None:
        outer = PostCommitChannel()
        outer.enqueue(lambda: log.append("outer_action"))
        token = bind_channel(outer)
        try:
            await service.execute()

            assert log == ["body", "commit", "session_closed", "owner", "dep_a", "dep_b"]
            assert active_channel() is outer

            await outer.drain(committed=True)
        finally:
            reset_channel(token)

        assert log[-1] == "outer_action"
