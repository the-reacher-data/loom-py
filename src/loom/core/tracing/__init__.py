"""Distributed trace-id propagation for Loom.

The active trace identifier is stored in a :class:`~contextvars.ContextVar`
so it propagates automatically across async call chains within a single
process.  Crossing process boundaries (HTTP, Kafka, Celery) requires
explicit serialisation — see the cross-service propagation guide.

Usage::

    from loom.core.tracing import get_trace_id, set_trace_id, reset_trace_id

    token = set_trace_id("abc123")
    try:
        result = await some_async_operation()
    finally:
        reset_trace_id(token)
"""

from loom.core.tracing.context import (
    generate_trace_id,
    get_trace_id,
    reset_trace_id,
    set_trace_id,
)

__all__ = [
    "generate_trace_id",
    "get_trace_id",
    "reset_trace_id",
    "set_trace_id",
]
