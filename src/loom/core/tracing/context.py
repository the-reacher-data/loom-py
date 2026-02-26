"""Active trace-id context guard.

Provides a ``contextvars.ContextVar`` that propagates a trace identifier
across the async execution stack — from the HTTP middleware that sets it,
through the use-case pipeline, down to SQLAlchemy queries and structured
log calls.

Rules:
    - Only the transport layer (HTTP middleware, Kafka consumer, Celery
      worker) calls :func:`set_trace_id`.
    - Any layer may call :func:`get_trace_id` without introducing upward
      coupling.
    - The token returned by :func:`set_trace_id` **must** be passed to
      :func:`reset_trace_id` in a ``finally`` block to restore the prior
      context.
"""

from __future__ import annotations

import uuid
from contextvars import ContextVar, Token

_trace_id: ContextVar[str | None] = ContextVar("_trace_id", default=None)


def get_trace_id() -> str | None:
    """Return the trace identifier active in the current async context.

    Returns:
        The active trace-id string, or ``None`` when no trace is set
        (e.g. background jobs, CLI commands, tests that do not inject one).
    """
    return _trace_id.get()


def set_trace_id(tid: str) -> Token[str | None]:
    """Set the trace identifier for the current async context.

    Args:
        tid: Trace identifier to activate.

    Returns:
        A :class:`~contextvars.Token` that must be passed to
        :func:`reset_trace_id` in a ``finally`` block.

    Example::

        token = set_trace_id("abc123")
        try:
            await handle_request()
        finally:
            reset_trace_id(token)
    """
    return _trace_id.set(tid)


def reset_trace_id(token: Token[str | None]) -> None:
    """Restore the trace identifier to its value before :func:`set_trace_id`.

    Args:
        token: Token returned by the corresponding :func:`set_trace_id` call.
    """
    _trace_id.reset(token)


def generate_trace_id() -> str:
    """Generate a new random trace identifier.

    Returns:
        A 32-character lowercase hex string (UUID4 without hyphens).

    Example::

        tid = generate_trace_id()  # e.g. "4b3f9a1c2d8e0f7b6a5c3e1d9f2b4a0c"
    """
    return uuid.uuid4().hex
