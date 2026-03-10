"""REST-layer constants: query parameter names and profile defaults."""

from __future__ import annotations

from enum import StrEnum


class QueryParam(StrEnum):
    """Public query parameter names for REST list/filter/pagination routes.

    All values are plain strings at runtime (``StrEnum``), so they can be
    passed directly to any API expecting ``str`` — e.g. ``request.query_params.get``.

    Example::

        page = int(query_params.get(QueryParam.PAGE, 1))
    """

    PAGE = "page"
    LIMIT = "limit"
    PAGINATION = "pagination"
    AFTER = "after"
    CURSOR = "cursor"
    SORT = "sort"
    DIRECTION = "direction"
    PROFILE = "profile"


PROFILE_DEFAULT = "default"
