from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from pytest import fixture


class MockSessionManager:
    def __init__(self, session: Any) -> None:
        self._session = session

    @asynccontextmanager
    async def session(self) -> Any:
        yield self._session


@fixture
def mock_session() -> AsyncMock:
    session = AsyncMock()
    session.new = set()
    session.dirty = set()
    session.deleted = set()
    return session


@fixture
def mock_session_manager(mock_session: AsyncMock) -> MockSessionManager:
    return MockSessionManager(mock_session)


@fixture
def fake_model() -> type:
    model = MagicMock()
    model.__name__ = "Dummy"
    return model
