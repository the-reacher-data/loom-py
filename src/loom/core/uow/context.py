"""Active UnitOfWork context guard.

Provides a ``contextvars.ContextVar`` that :class:`~loom.core.engine.executor.RuntimeExecutor`
uses to detect nested executions and skip opening a redundant second UoW.

No backend-specific types are imported here.
"""

from __future__ import annotations

import contextvars
from typing import Any

# Holds the currently active UnitOfWork within an async execution context.
# Set to the UoW instance by RuntimeExecutor when it owns the transaction.
# None when no active UoW is managed by the executor.
_active_uow: contextvars.ContextVar[Any] = contextvars.ContextVar("_active_uow", default=None)


def get_active_uow() -> Any:
    """Return the UnitOfWork active in the current async context, or ``None``.

    Returns:
        The active :class:`~loom.core.uow.abc.UnitOfWork` if inside an
        executor-managed transaction, otherwise ``None``.
    """
    return _active_uow.get()
