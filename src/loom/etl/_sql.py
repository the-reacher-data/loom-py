"""SQL template resolver — safe ``{{ params.x.y }}`` interpolation.

Resolves ``{{ params.a.b.c }}`` placeholders in SQL strings against a
concrete params instance.  Only attribute-chain expressions starting with
``params`` are allowed — no arbitrary code execution.

Values are escaped as safe SQL literals so strings are quoted, booleans
are ``TRUE``/``FALSE``, and dates are ISO-quoted.

Internal module — not part of the public API.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

_PLACEHOLDER = re.compile(r"\{\{([^{}]+)\}\}")


def resolve_sql(sql: str, params: Any) -> str:
    """Resolve ``{{ params.x.y }}`` placeholders in *sql*.

    Args:
        sql:    SQL string with zero or more ``{{ params.x.y }}`` placeholders.
        params: Concrete params instance whose attributes are resolved.

    Returns:
        SQL string with all placeholders replaced by safe literal values.

    Raises:
        ValueError:    If a placeholder is not a valid ``params.*`` chain.
        AttributeError: If an attribute in the chain does not exist on params.

    Example::

        sql = "SELECT * FROM orders WHERE year = {{ params.run_date.year }}"
        result = resolve_sql(sql, DailyParams(run_date=date(2024, 1, 15)))
        # → "SELECT * FROM orders WHERE year = 2024"
    """
    return _PLACEHOLDER.sub(lambda m: _resolve_placeholder(m.group(1).strip(), params), sql)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _resolve_placeholder(expr: str, params: Any) -> str:
    parts = expr.split(".")
    if parts[0] != "params":
        raise ValueError(f"resolve_sql: only 'params.x.y' expressions allowed, got: {expr!r}")
    if not all(p.isidentifier() for p in parts):
        raise ValueError(f"resolve_sql: invalid expression: {expr!r}")
    value: Any = params
    for attr in parts[1:]:
        value = getattr(value, attr)
    return _sql_literal(value)


def _sql_literal(value: Any) -> str:
    """Serialize *value* as a safe SQL literal."""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, datetime):
        return f"'{value.isoformat()}'"
    if isinstance(value, date):
        return f"'{value.isoformat()}'"
    if isinstance(value, str):
        return f"'{value.replace(chr(39), chr(39) * 2)}'"
    return str(value)
