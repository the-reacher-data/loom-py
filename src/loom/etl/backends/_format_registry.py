"""Shared helpers for backend file-format dispatch and write options."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TypeVar

from loom.core.errors.errors import LoomError
from loom.etl.declarative._format import Format
from loom.etl.declarative._write_options import WriteOptions

_HandlerT = TypeVar("_HandlerT")
_OptionsT = TypeVar("_OptionsT")
"""Options type of one format; ``WriteOptions`` is a union, so it cannot bound it."""


class UnsupportedFormatError(LoomError, ValueError):
    """Raised when no handler of the backend implements a declared file format.

    Also a :class:`ValueError`, so call-sites that only care that the format is
    invalid keep working without knowing the framework error hierarchy.

    Args:
        fmt:       Format the pipeline declared.
        supported: Formats the backend registered for this operation.

    Example::

        raise UnsupportedFormatError(Format.XLSX, (Format.CSV, Format.PARQUET))
    """

    def __init__(self, fmt: Format, supported: Iterable[Format]) -> None:
        names = ", ".join(sorted(item.value for item in supported)) or "none"
        super().__init__(
            f"Unsupported format: {fmt.value}. Supported here: {names}.",
            code="unsupported_format",
        )
        self.format = fmt


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
        ValueError: If the format string is not a known
            :class:`~loom.etl.declarative._format.Format`.
        UnsupportedFormatError: If the format is not registered in *handlers*.
    """
    fmt = format_value if isinstance(format_value, Format) else Format(format_value)
    handler = handlers.get(fmt)
    if handler is None:
        raise UnsupportedFormatError(fmt, handlers)
    return handler


def write_options_or_default(options: WriteOptions | None, expected: type[_OptionsT]) -> _OptionsT:
    """Return *options* when it is of the *expected* type, else its defaults.

    A target declares one options object for one format, so a mismatch means the
    declaration belongs to another format and carries nothing this writer can
    honour.

    Args:
        options:  Options declared on the target, if any.
        expected: Options type of the format being written.

    Returns:
        The declared options, or a default-constructed *expected*.
    """
    if isinstance(options, expected):
        return options
    return expected()


__all__ = ["UnsupportedFormatError", "resolve_format_handler", "write_options_or_default"]
