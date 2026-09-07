from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T", bound=type[object])
F = TypeVar("F", bound=Callable[..., object])


def cached(cls: T) -> T:
    """Declarative marker for repositories that support cache wrapping."""
    cls.__cache_policy__ = True  # type: ignore[attr-defined]
    return cls


def cache_query(
    *,
    scope: str = "list",
    ttl_key: str | None = None,
) -> Callable[[F], F]:
    """Declarative marker for custom repository read methods.

    Treat the returned value as immutable.  Concurrent callers that miss
    together are served the *same object* by the coalesced load, while a
    caller served from the cache gets a freshly decoded one, so mutating a
    result makes the two paths disagree.  Return a struct, or a fresh copy.

    Args:
        scope: ``"entity"`` for a single-entity read, ``"list"`` otherwise;
            decides which tags invalidate the entry and which TTL applies.
        ttl_key: Entity name whose TTL override applies, when the method
            caches something other than its own entity.

    Returns:
        The decorator that marks the method.
    """

    def decorator(func: F) -> F:
        func.__cache_query__ = {"scope": scope, "ttl_key": ttl_key}  # type: ignore[attr-defined]
        return func

    return decorator
