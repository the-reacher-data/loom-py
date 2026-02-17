from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from functools import wraps
from typing import Any, Generic, TypeVar, cast

from sqlalchemy.ext.asyncio import AsyncSession

from loom.core.logger import get_logger
from loom.core.repository.abc import IdT, OutputT
from loom.core.repository.mutation import MutationEvent
from loom.core.repository.sqlalchemy.mixins import (
    SQLAlchemyCreateMixin,
    SQLAlchemyDeleteMixin,
    SQLAlchemyReadMixin,
    SQLAlchemyUpdateMixin,
)
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.transactional import get_active_session

R = TypeVar("R")


def with_session_scope(
    method: Callable[..., Awaitable[R]],
) -> Callable[..., Awaitable[R]]:
    """Inject repository-managed session into custom repository methods."""

    @wraps(method)
    async def wrapper(
        self: RepositorySQLAlchemy[Any, Any],
        *args: Any,
        session: AsyncSession | None = None,
        **kwargs: Any,
    ) -> R:
        async with self._session_scope(session) as scoped_session:
            return await method(self, scoped_session, *args, **kwargs)

    return cast(Callable[..., Awaitable[R]], wrapper)


class RepositorySQLAlchemy(
    SQLAlchemyCreateMixin,
    SQLAlchemyReadMixin,
    SQLAlchemyUpdateMixin,
    SQLAlchemyDeleteMixin,
    Generic[OutputT, IdT],
):
    """Base SQLAlchemy repository with context-aware session management."""

    def __init__(self, session_manager: SessionManager) -> None:
        """Initialise the repository with a session manager.

        Args:
            session_manager: Manages async database sessions and connection pooling.
        """
        self.session_manager = session_manager
        self.log = get_logger(__name__).bind(repository=self.__class__.__name__)

    async def on_transaction_committed(self, events: tuple[MutationEvent, ...]) -> None:
        """Handle post-commit mutation events (cache invalidation hook)."""
        self.log.debug("RepositoryTransactionCommitted", mutation_count=len(events))

    @asynccontextmanager
    async def _session_scope(
        self, session: AsyncSession | None = None
    ) -> AsyncIterator[AsyncSession]:
        """Reuse active transaction session or create a scoped one."""
        if session is not None:
            yield session
            return

        context_session = get_active_session()
        if context_session is not None:
            yield context_session
            return

        async with self.session_manager.session() as new_session:
            try:
                yield new_session
                await new_session.commit()
            except Exception:
                await new_session.rollback()
                self.log.exception("RepositorySessionRollback")
                raise
