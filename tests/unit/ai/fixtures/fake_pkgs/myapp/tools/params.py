"""Factories exercising how ``params`` bind to a factory's signature."""

from __future__ import annotations

from typing import Any


def build_any(context: object, **options: Any) -> object:
    """Accept any keyword parameter through ``**options``."""
    del context, options
    return object()


def build_strict(context: object, *, max_results: int) -> object:
    """Require ``max_results``; the artifact must supply it."""
    del context, max_results
    return object()


class _Uninspectable:
    """Callable whose ``__signature__`` makes ``inspect.signature`` raise."""

    __signature__ = "not a signature"

    def __call__(self, context: object, **options: Any) -> object:
        """Build a toolset whatever the parameters are."""
        del context, options
        return object()


UNINSPECTABLE = _Uninspectable()
"""Callable ``inspect.signature`` cannot describe; accepted with any ``params``."""
