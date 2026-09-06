"""Unit of work for the Mongo backend.

MongoDB transactions need a replica set and a client session, so they are
opt-in (``persistence.mongo.transactions: true``). The unit of work is one
class; what happens at each boundary is a :class:`SessionScope`:

- :class:`NoSessionScope` (default) opens nothing, mirroring
  :class:`~loom.core.repository.dynamodb.uow.DynamoUnitOfWork`: each driver
  call is autocommitted and the repository receives no session.
- :class:`TransactionScope` starts a client session and a transaction in
  ``begin``, commits or aborts it, and ends the session afterwards.

The scope's session is published through a ``ContextVar`` that
:func:`active_session` reads, so a
:class:`~loom.core.repository.mongo.repository.RepositoryMongo` built with
``session_provider=active_session`` joins the transaction of the use case it
runs in. The token is reset on exit, as the SQLAlchemy unit of work does.

``pymongo`` is imported at module level: this module is only reached through
the Mongo backend, and the persistence registry turns an ``ImportError`` at
entry-point load into a ``ConfigError`` naming the extra.
"""

from __future__ import annotations

import contextvars
from collections.abc import Callable
from typing import Protocol

from pymongo.asynchronous.client_session import AsyncClientSession

from loom.core.logger import get_logger
from loom.core.uow.abc import UnitOfWork

_log = get_logger(__name__).bind(component="uow")

_active_session: contextvars.ContextVar[AsyncClientSession | None] = contextvars.ContextVar(
    "loom_mongo_active_session", default=None
)


def active_session() -> AsyncClientSession | None:
    """Return the client session of the enclosing unit of work, or ``None``.

    Pass it as ``session_provider`` to
    :class:`~loom.core.repository.mongo.repository.RepositoryMongo`.
    """
    return _active_session.get()


class SessionClient(Protocol):
    """The subset of ``AsyncMongoClient`` the transactional scope drives."""

    def start_session(self) -> AsyncClientSession:
        """Return a new client session; no I/O until it is used."""
        ...


class SessionScope(Protocol):
    """What one unit of work does at each transaction boundary.

    A scope belongs to a single unit of work: ``begin`` returns the session
    the repositories must use (``None`` when there is none), and the other
    three act on that same session.
    """

    async def begin(self) -> AsyncClientSession | None:
        """Open the session, if any, and return it."""
        ...

    async def commit(self) -> None:
        """Make the writes of the session durable."""
        ...

    async def abort(self) -> None:
        """Discard the writes of the session."""
        ...

    async def end(self) -> None:
        """Release the session."""
        ...


class NoSessionScope:
    """Scope for ``transactions: false``: no session, every write autocommits."""

    async def begin(self) -> None:
        """Open nothing."""
        return None

    async def commit(self) -> None:
        """No-op: each driver call is already committed."""

    async def abort(self) -> None:
        """No-op: autocommitted writes cannot be discarded here."""

    async def end(self) -> None:
        """No-op: there is no session to release."""


class TransactionScope:
    """Scope for ``transactions: true``: one client session and one transaction.

    Args:
        client: Client the session is started on.
    """

    def __init__(self, client: SessionClient) -> None:
        self._client = client
        self._session: AsyncClientSession | None = None

    async def begin(self) -> AsyncClientSession:
        """Start a session and a transaction on it.

        The session is kept before the transaction starts so :meth:`end`
        releases it even when ``start_transaction`` fails.
        """
        self._session = self._client.start_session()
        await self._session.start_transaction()
        return self._session

    async def commit(self) -> None:
        """Commit the transaction."""
        await self._require_session().commit_transaction()

    async def abort(self) -> None:
        """Abort the transaction."""
        await self._require_session().abort_transaction()

    async def end(self) -> None:
        """End the session."""
        session = self._session
        self._session = None
        if session is not None:
            await session.end_session()

    def _require_session(self) -> AsyncClientSession:
        if self._session is None:
            raise RuntimeError("TransactionScope used before begin()")
        return self._session


class MongoUnitOfWork:
    """Unit of work binding a :class:`SessionScope` to the current context.

    Args:
        scope: Boundary strategy owned by this unit of work.

    Example::

        uow = MongoUnitOfWork(TransactionScope(client))
        async with uow:
            await repo.create(entity)
        # committed
    """

    def __init__(self, scope: SessionScope) -> None:
        self._scope = scope
        self._token: contextvars.Token[AsyncClientSession | None] | None = None

    async def begin(self) -> None:
        """Open the scope and publish its session to :func:`active_session`.

        Raises:
            RuntimeError: If called twice without a ``commit`` or ``rollback``.
        """
        if self._token is not None:
            raise RuntimeError("MongoUnitOfWork.begin() called twice without commit/rollback")
        try:
            session = await self._scope.begin()
        except Exception:
            await self._scope.end()
            raise
        self._token = _active_session.set(session)
        _log.debug("UoWBegin")

    async def commit(self) -> None:
        """Commit the scope.

        Raises:
            RuntimeError: If ``begin`` has not been called.
        """
        self._require_begun("commit")
        await self._scope.commit()
        _log.debug("UoWCommit")

    async def rollback(self) -> None:
        """Abort the scope.

        Raises:
            RuntimeError: If ``begin`` has not been called.
        """
        self._require_begun("rollback")
        await self._scope.abort()
        _log.debug("UoWRollback")

    async def __aenter__(self) -> MongoUnitOfWork:
        """Begin the unit of work and return it."""
        await self.begin()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Commit on clean exit, roll back on exception, always close."""
        try:
            if exc_type is None:
                await self.commit()
            else:
                await self._rollback_logging_failure()
        finally:
            await self._close()

    async def _rollback_logging_failure(self) -> None:
        try:
            await self.rollback()
        except Exception:
            _log.exception("UoWRollbackFailed")

    async def _close(self) -> None:
        """End the scope and unpublish the session.

        A failure while ending is logged, not raised: it would otherwise
        replace the exception the use case is already propagating.
        """
        try:
            await self._scope.end()
        except Exception:
            _log.exception("UoWCloseFailed")
        finally:
            if self._token is not None:
                _active_session.reset(self._token)
                self._token = None

    def _require_begun(self, operation: str) -> None:
        if self._token is None:
            raise RuntimeError(f"MongoUnitOfWork.{operation}() called before begin()")


class MongoUnitOfWorkFactory:
    """Factory that creates fresh :class:`MongoUnitOfWork` instances.

    Build it through :meth:`without_transactions` or :meth:`transactional`;
    register one instance and inject it into
    :class:`~loom.core.engine.executor.RuntimeExecutor` at startup.

    Args:
        new_scope: Returns the scope each unit of work owns.
    """

    def __init__(self, new_scope: Callable[[], SessionScope]) -> None:
        self._new_scope = new_scope

    @classmethod
    def without_transactions(cls) -> MongoUnitOfWorkFactory:
        """Return a factory whose units of work open no session."""
        return cls(NoSessionScope)

    @classmethod
    def transactional(cls, client: SessionClient) -> MongoUnitOfWorkFactory:
        """Return a factory whose units of work run one transaction on ``client``.

        Args:
            client: Client sessions are started on; needs a replica set.
        """
        return cls(lambda: TransactionScope(client))

    def create(self) -> UnitOfWork:
        """Return a fresh, not-yet-begun :class:`UnitOfWork`."""
        return MongoUnitOfWork(self._new_scope())


__all__ = [
    "MongoUnitOfWork",
    "MongoUnitOfWorkFactory",
    "NoSessionScope",
    "SessionClient",
    "SessionScope",
    "TransactionScope",
    "active_session",
]
