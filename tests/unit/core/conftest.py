"""Session doubles shared by the unit-of-work and SQLAlchemy suites.

The unit of work and the ``@transactional`` decorator both drive a session
through a session manager, so the controllable session and the manager that
records its scope exits live here, once, for both subtrees.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest


def make_session() -> MagicMock:
    """Return a session double whose ``commit``/``rollback``/``close`` are awaitable."""
    session = MagicMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.close = AsyncMock()
    return session


class RecordingSessionManager:
    """Session manager double whose scope counts its exits and may fail to close.

    Args:
        session: Session yielded by the scope.
        exit_error: Raised when the scope closes, standing in for a broken pool.
        log: Appended with ``"session_closed"`` on every exit, so a test can
            order the close against the post-commit hooks.
    """

    def __init__(
        self,
        session: Any,
        *,
        exit_error: Exception | None = None,
        log: list[str] | None = None,
    ) -> None:
        self._session = session
        self._exit_error = exit_error
        self._log = log
        self.exits = 0

    def session(self) -> RecordingSessionManager:
        return self

    async def __aenter__(self) -> Any:
        return self._session

    async def __aexit__(self, *args: object) -> None:
        self.exits += 1
        if self._log is not None:
            self._log.append("session_closed")
        if self._exit_error is not None:
            raise self._exit_error


@pytest.fixture
def session_double() -> MagicMock:
    """A fresh session double with awaitable ``commit``/``rollback``/``close``."""
    return make_session()
