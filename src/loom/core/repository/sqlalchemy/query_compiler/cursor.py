"""Keyset predicate compilation and next-cursor extraction for SQLAlchemy.

Tokens follow :mod:`loom.core.repository.abc.cursor`: every sort key plus
the primary key as the final tie-breaker.

The N+1 trick:  fetch ``limit + 1`` rows.  If the result set has exactly
``limit + 1`` items, a next page exists — truncate to ``limit`` and encode
the last item's keys.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import and_, or_

from loom.core.repository.abc.cursor import Cursor, encode_cursor
from loom.core.repository.abc.query import SortSpec
from loom.core.repository.sqlalchemy.query_compiler.paths import resolve_column


def compile_cursor_predicate(
    sa_model: type[Any],
    cursor: Cursor,
    sort: tuple[SortSpec, ...],
    id_column: Any,
) -> Any:
    """Build a WHERE predicate that positions the query after ``cursor``.

    The predicate is the row-value comparison expanded as an OR of ANDs,
    which every dialect (including sqlite) accepts:
    ``k1 > v1 OR (k1 = v1 AND k2 < v2) OR (... AND id > vid)``.

    Args:
        sa_model: Root SQLAlchemy mapped model class.
        cursor: Decoded cursor whose keys match ``sort`` one to one.
        sort: Sort directives the page is ordered by.
        id_column: Primary-key column used as the ascending tie-breaker.

    Returns:
        SQLAlchemy boolean expression.

    Raises:
        FilterPathError: If a sort field cannot be resolved.
    """
    columns = [resolve_column(sa_model, spec.field) for spec in sort] + [id_column]
    directions = [spec.direction for spec in sort] + ["ASC"]
    values = [*cursor.keys, cursor.tie_breaker]
    branches: list[Any] = []
    for index, (column, direction, value) in enumerate(
        zip(columns, directions, values, strict=True)
    ):
        step = column > value if direction == "ASC" else column < value
        equal_prefix = [columns[j] == values[j] for j in range(index)]
        branches.append(and_(*equal_prefix, step))
    return or_(*branches)


def extract_next_cursor(
    items: list[Any],
    sort: tuple[SortSpec, ...],
    id_attribute: str,
    limit: int,
    backend: str,
) -> tuple[list[Any], str | None, bool]:
    """Apply the N+1 trick to detect the next page and build its cursor.

    Call this after fetching ``limit + 1`` rows.

    Args:
        items: Raw ORM objects fetched (may contain up to ``limit + 1``).
        sort: Sort directives whose fields are read from the last row.
        id_attribute: Primary-key attribute name on the ORM object.
        limit: Requested page size.
        backend: Backend name stamped on the token.

    Returns:
        Tuple of ``(page_items, next_cursor_token, has_next)``.
        ``page_items`` is truncated to ``limit``.
        ``next_cursor_token`` is ``None`` on the last page.
    """
    has_next = len(items) > limit
    page_items = items[:limit]
    if not has_next:
        return page_items, None, has_next
    last = page_items[-1]
    keys = [_read_path(last, spec.field) for spec in sort]
    return page_items, encode_cursor(backend, keys, getattr(last, id_attribute)), has_next


def _read_path(obj: Any, path: str) -> Any:
    value = obj
    for segment in path.split("."):
        value = getattr(value, segment)
    return value
