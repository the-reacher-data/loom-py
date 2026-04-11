"""Shared helpers for backend file-format dispatch."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TypeVar

from loom.etl.declarative._format import Format

_HandlerT = TypeVar("_HandlerT")


def resolve_format_handler(
    format_value: Format | str,
    handlers: Mapping[Format, _HandlerT],
) -> _HandlerT:
    """Return the registered handler for a file format.

    Args:
        format_value: File format enum value or raw string value.
        handlers: Mapping from :class:`~loom.etl.declarative._format.Format` to
            a handler object/callable.

    Returns:
        Registered handler for ``format_value``.

    Raises:
        ValueError: If format is unknown or not registered in *handlers*.
    """
    fmt = format_value if isinstance(format_value, Format) else Format(format_value)
    handler = handlers.get(fmt)
    if handler is None:
        raise ValueError(f"Unsupported format: {fmt.value}")
    return handler


__all__ = ["resolve_format_handler"]
