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

    Annotate the return type.  The wrapper derives a codec from it and applies
    it to the cached read and to the fresh one alike, so a hit and a miss
    return the same type; the supported grammar is a :class:`msgspec.Struct`,
    a scalar, or a ``list``, ``tuple`` or optional of those.  A return type
    outside it — a mapping, a generic container, ``Any``, a forward reference
    the defining module cannot resolve — emits a ``DeprecationWarning`` when
    the repository is wrapped, and keeps the old behaviour, where the cached
    call returns the decoded payload rather than the declared type.

    The declared type is what the caller gets, on the fresh call as on the
    cached one: a method annotated ``-> Stats`` that returns a subclass of
    ``Stats`` hands back a narrowed ``Stats``, and the fields the subclass
    added are dropped.  Declare the type you mean to return.

    A method that returns ``None`` is not cached: the backend cannot tell a
    stored ``None`` from a miss, so the read runs again next time.  A cached
    payload that no longer fits the declared type — an older deployment wrote
    it, and the type has since gained a field — is treated as a miss and
    overwritten, not raised to the caller.

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
