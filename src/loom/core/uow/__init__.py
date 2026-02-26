"""Unit of Work port — transport and backend agnostic.

Defines the :class:`UnitOfWork` and :class:`UnitOfWorkFactory` protocols
that decouple the application layer from the underlying session management
(SQLAlchemy, etc.).

:class:`~loom.core.engine.executor.RuntimeExecutor` drives the UoW lifecycle
(begin/commit/rollback) around each UseCase execution.  Nested executions
reuse the same instance via a ``contextvars.ContextVar`` guard.
"""

from loom.core.uow.abc import UnitOfWork, UnitOfWorkFactory

__all__ = ["UnitOfWork", "UnitOfWorkFactory"]
