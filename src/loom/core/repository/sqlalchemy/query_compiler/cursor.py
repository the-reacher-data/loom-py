"""Cursor encode / decode and predicate compilation.

Cursors are opaque base64-encoded JSON tokens of the form
``{"field": value, ...}``.  The encoded value is safe to include in URLs.

The N+1 trick:  fetch ``limit + 1`` rows.  If the result set has exactly
``limit + 1`` items, a next page exists — truncate to ``limit`` and encode
the last item's cursor value.
"""

from __future__ import annotations

import base64
import json
from typing import Any, cast

import msgspec

from loom.core.repository.sqlalchemy.query_compiler.paths import resolve_column


def encode_cursor(field: str, value: Any) -> str:
    """Encode a cursor token for the given field and value.

    Args:
        field: Name of the sort/cursor field.
        value: Value of that field for the last returned item.

    Returns:
        URL-safe base64-encoded JSON string.

    Example::

        token = encode_cursor("id", 42)
        # "eyJpZCI6IDQyfQ=="
    """
    raw = json.dumps({field: value}, default=str)
    return base64.urlsafe_b64encode(raw.encode()).decode()


def decode_cursor(token: str) -> dict[str, Any]:
    """Decode an opaque cursor token back to its field-value mapping.

    Args:
        token: URL-safe base64-encoded JSON string produced by
            :func:`encode_cursor`.

    Returns:
        Dict with a single ``{field: value}`` entry.

    Raises:
        ValueError: If the token is malformed or not valid base64/JSON.
    """
    try:
        raw = base64.urlsafe_b64decode(token.encode())
        decoded = json.loads(raw)
        if not isinstance(decoded, dict):
            raise ValueError("Decoded cursor payload must be an object.")
        return cast(dict[str, Any], decoded)
    except Exception as exc:
        raise ValueError(f"Invalid cursor token: {exc}") from exc


def compile_cursor_predicate(
    sa_model: type[Any],
    cursor_token: str,
) -> Any:
    """Build a WHERE predicate that positions the query after the cursor.

    Assumes ascending order on the cursor field (``field > cursor_value``).

    Args:
        sa_model: Root SQLAlchemy mapped model class.
        cursor_token: Opaque cursor from :func:`encode_cursor`.

    Returns:
        SQLAlchemy binary expression (``column > value``).

    Raises:
        ValueError: If the cursor token is malformed.
        FilterPathError: If the cursor field cannot be resolved.
    """
    decoded = decode_cursor(cursor_token)
    field, value = next(iter(decoded.items()))
    col = resolve_column(sa_model, field)
    return col > value


def extract_next_cursor(
    items: list[Any],
    cursor_field: str,
    limit: int,
) -> tuple[list[Any], str | None, bool]:
    """Apply the N+1 trick to detect the next page and build its cursor.

    Call this after fetching ``limit + 1`` rows.

    Args:
        items: Raw ORM objects fetched (may contain up to ``limit + 1``).
        cursor_field: Attribute name on the ORM object used as cursor key.
        limit: Requested page size.

    Returns:
        Tuple of ``(page_items, next_cursor_token, has_next)``.
        ``page_items`` is truncated to ``limit``.
        ``next_cursor_token`` is ``None`` on the last page.
    """
    has_next = len(items) > limit
    page_items = items[:limit]
    next_cursor: str | None = None
    if has_next:
        last = page_items[-1]
        value = getattr(last, cursor_field)
        encoded_value = msgspec.to_builtins(value) if hasattr(value, "__struct_fields__") else value
        next_cursor = encode_cursor(cursor_field, encoded_value)
    return page_items, next_cursor, has_next
