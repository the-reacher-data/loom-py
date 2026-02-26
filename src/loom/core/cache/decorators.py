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
    """Declarative marker for custom repository read methods."""

    def decorator(func: F) -> F:
        func.__cache_query__ = {"scope": scope, "ttl_key": ttl_key}  # type: ignore[attr-defined]
        return func

    return decorator
