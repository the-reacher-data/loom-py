"""Placeholder DSL for dynamic ETL parameters.

Supported tokens (case-sensitive):

- ``${today}``, ``${today+Nd}``, ``${today-Nd}`` → :class:`datetime.date`
- ``${yesterday}`` → :class:`datetime.date` (alias of ``${today-1d}``)
- ``${now}``, ``${now±Nd}``, ``${now±Nh}``, ``${now±Nm}`` →
  timezone-aware UTC :class:`datetime.datetime`

Any value that looks like ``${...}`` but does not match one of the three
valid patterns raises :class:`ValueError`. Non-string values and strings
that do not look like placeholders pass through unchanged.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from typing import Any

_TODAY_RE = re.compile(r"^\$\{today(?:([+-])(\d+)d)?\}$")
_YESTERDAY_RE = re.compile(r"^\$\{yesterday\}$")
_NOW_RE = re.compile(r"^\$\{now(?:([+-])(\d+)([dhm]))?\}$")
_ANY_DOLLAR = re.compile(r"^\$\{.*\}$")


def resolve_placeholder(value: Any) -> Any:
    """Resolve a placeholder string to a concrete ``date`` / ``datetime``.

    Args:
        value: Arbitrary parameter value. Only strings matching the DSL are
            resolved; every other value is returned unchanged.

    Returns:
        The resolved date/datetime, or the original value if it is not a
        placeholder.

    Raises:
        ValueError: If the value looks like a placeholder (``${...}``) but
            does not match any of the supported patterns.
    """
    if not isinstance(value, str):
        return value
    if not value.startswith("${"):
        return value

    today_match = _TODAY_RE.match(value)
    if today_match is not None:
        sign, days = today_match.groups()
        delta = 0 if sign is None else int(days) * (1 if sign == "+" else -1)
        return datetime.now(UTC).date() + timedelta(days=delta)

    if _YESTERDAY_RE.match(value) is not None:
        return datetime.now(UTC).date() - timedelta(days=1)

    now_match = _NOW_RE.match(value)
    if now_match is not None:
        sign, amount, unit = now_match.groups()
        now = datetime.now(UTC)
        if sign is None:
            return now
        value_int = int(amount) * (1 if sign == "+" else -1)
        if unit == "d":
            return now + timedelta(days=value_int)
        if unit == "h":
            return now + timedelta(hours=value_int)
        return now + timedelta(minutes=value_int)

    if _ANY_DOLLAR.match(value) is not None:
        raise ValueError(f"invalid placeholder: {value!r}")
    return value


__all__ = ["resolve_placeholder"]
