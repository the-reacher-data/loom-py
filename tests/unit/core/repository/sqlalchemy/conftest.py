from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Annotated, Any
from unittest.mock import AsyncMock

from pytest import fixture

from loom.core.backend.sqlalchemy import compile_all, reset_registry
from loom.core.model import BaseModel, Field, Integer


class _DummyModel(BaseModel):
    __tablename__ = "dummies"
    id: Annotated[int, Integer, Field(primary_key=True, autoincrement=True)]


class MockSessionManager:
    def __init__(self, session: Any) -> None:
        self._session = session

    @asynccontextmanager
    async def session(self) -> Any:
        yield self._session


@fixture(autouse=True)
def _compiled_dummy_model() -> Any:
    reset_registry()
    compile_all(_DummyModel)
    yield
    reset_registry()


@fixture
def dummy_model() -> type:
    return _DummyModel


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
