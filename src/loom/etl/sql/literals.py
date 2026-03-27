"""Shared SQL literal rendering helpers."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any


def sql_literal(value: Any) -> str:
    """Render a Python scalar as a SQL literal.

    Args:
        value: Python scalar to render.

    Returns:
        SQL literal string.
    """
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, datetime):
        return f"'{value.isoformat()}'"
    if isinstance(value, date):
        return f"'{value.isoformat()}'"
    if isinstance(value, str):
        return f"'{value.replace(chr(39), chr(39) * 2)}'"
    return str(value)
