from __future__ import annotations

import contextvars
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any, Concatenate, ParamSpec, Protocol, TypeVar, cast, runtime_checkable

from sqlalchemy.ext.asyncio import AsyncSession

from loom.core.logger import get_logger
from loom.core.repository.mutation import MutationEvent


@runtime_checkable
class SupportsPostCommit(Protocol):
    """Protocol for objects that react to committed transactions."""

    async def on_transaction_committed(self, events: tuple[MutationEvent, ...]) -> None: ...


T = TypeVar("T")
P = ParamSpec("P")

_active_session: contextvars.ContextVar[AsyncSession | None] = contextvars.ContextVar(
    "_active_session",
    default=None,
)
_mutations: contextvars.ContextVar[list[MutationEvent] | None] = contextvars.ContextVar(
    "_mutations",
    default=None,
)
_log = get_logger(__name__).bind(component="transactional")


def get_active_session() -> AsyncSession | None:
    """Return the transactional session bound to the current context, or ``None``.

    Returns:
        The active ``AsyncSession`` if inside a ``@transactional`` scope, else ``None``.
    """
    return _active_session.get()


def record_mutation(event: MutationEvent) -> None:
    """Append a mutation event to the current transaction's pending list.

    If called outside a ``@transactional`` scope the event is silently discarded.

    Args:
        event: The mutation event to record.
    """
    events = _mutations.get()
    if events is None:
        return
    events.append(event)


def get_pending_mutations() -> tuple[MutationEvent, ...]:
    """Return all mutation events recorded in the current transaction scope.

    Returns:
        A tuple of ``MutationEvent`` instances, empty if none were recorded.
    """
    events = _mutations.get()
    if not events:
        return ()
    return tuple(events)


def transactional(
    method: Callable[Concatenate[Any, P], Awaitable[T]],
) -> Callable[Concatenate[Any, P], Awaitable[T]]:
    """Create a single transaction boundary for service/orchestrator use cases."""

    @wraps(method)
    async def wrapper(self: Any, *args: Any, **kwargs: Any) -> T:
        from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy

        if isinstance(self, RepositorySQLAlchemy):
            raise TypeError(
                "@transactional is intended for service/orchestrator boundaries, "
                "not repository methods.",
            )

        existing_session = get_active_session()
        if existing_session is not None:
            _log.debug(
                "TransactionalSessionReused",
                owner=self.__class__.__name__,
                method=method.__name__,
            )
            return await method(self, *args, **kwargs)

        session_manager = getattr(self, "session_manager", None)
        if session_manager is None or not callable(getattr(session_manager, "session", None)):
            raise TypeError(
                f"{self.__class__.__name__} must have a 'session_manager' attribute "
                f"with a .session() context manager to use @transactional.",
            )

        async with session_manager.session() as session:
            session_token = _active_session.set(session)
            mutations_token = _mutations.set([])
            try:
                result = await method(self, *args, **kwargs)
                await session.commit()
                _log.info(
                    "TransactionCommitted",
                    owner=self.__class__.__name__,
                    method=method.__name__,
                    mutation_count=len(get_pending_mutations()),
                )

                pending = get_pending_mutations()
                if isinstance(self, SupportsPostCommit):
                    await self.on_transaction_committed(pending)
                for dependency in _iter_post_commit_dependencies(self):
                    await dependency.on_transaction_committed(pending)
                return result
            except Exception:
                await session.rollback()
                _log.exception(
                    "TransactionRolledBack",
                    owner=self.__class__.__name__,
                    method=method.__name__,
                )
                raise
            finally:
                _active_session.reset(session_token)
                _mutations.reset(mutations_token)

    return cast(Callable[Concatenate[Any, P], Awaitable[T]], wrapper)


def _iter_post_commit_dependencies(owner: Any) -> list[SupportsPostCommit]:
    dependencies: list[SupportsPostCommit] = []
    for value in vars(owner).values():
        if value is owner:
            continue
        if isinstance(value, SupportsPostCommit):
            dependencies.append(value)
    return dependencies
