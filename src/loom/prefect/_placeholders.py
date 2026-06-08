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

_NOW_UNIT_KW = {"d": "days", "h": "hours", "m": "minutes"}


def _signed_int(sign: str | None, amount: str | None) -> int:
    if sign is None or amount is None:
        return 0
    magnitude = int(amount)
    return magnitude if sign == "+" else -magnitude


def _resolve_today(match: re.Match[str]) -> Any:
    sign, days = match.groups()
    return datetime.now(UTC).date() + timedelta(days=_signed_int(sign, days))


def _resolve_now(match: re.Match[str]) -> Any:
    sign, amount, unit = match.groups()
    now = datetime.now(UTC)
    if sign is None:
        return now
    return now + timedelta(**{_NOW_UNIT_KW[unit]: _signed_int(sign, amount)})


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
    if not isinstance(value, str) or not value.startswith("${"):
        return value

    today_match = _TODAY_RE.match(value)
    if today_match is not None:
        return _resolve_today(today_match)

    if _YESTERDAY_RE.match(value) is not None:
        return datetime.now(UTC).date() - timedelta(days=1)

    now_match = _NOW_RE.match(value)
    if now_match is not None:
        return _resolve_now(now_match)

    if _ANY_DOLLAR.match(value) is not None:
        raise ValueError(f"invalid placeholder: {value!r}")
    return value


__all__ = ["resolve_placeholder"]
