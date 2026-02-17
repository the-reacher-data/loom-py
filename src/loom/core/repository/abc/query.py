from __future__ import annotations

from typing import Any, Generic, TypeVar

import msgspec

OutputT = TypeVar("OutputT", bound=msgspec.Struct, covariant=True)


class PageParams(msgspec.Struct, kw_only=True):
    """Pagination parameters for list queries.

    Attributes:
        page: 1-based page number.
        limit: Maximum number of items per page (1-1000).
    """

    page: int = 1
    limit: int = 50

    def __post_init__(self) -> None:
        if self.page < 1:
            raise ValueError("page must be >= 1")
        if not (1 <= self.limit <= 1000):
            raise ValueError("limit must be in [1, 1000]")

    @property
    def offset(self) -> int:
        """Calculate the zero-based row offset for the current page."""
        return (self.page - 1) * self.limit


class FilterParams(msgspec.Struct, kw_only=True):
    """Generic filter container for list queries.

    Attributes:
        filters: Flat key-value mapping passed as ``filter_by`` kwargs to SQLAlchemy.
    """

    filters: dict[str, Any] = msgspec.field(default_factory=dict)


class PageResult(msgspec.Struct, Generic[OutputT], kw_only=True):
    """Paginated result set returned by list queries.

    Attributes:
        items: Tuple of entity output structs for the current page.
        total_count: Total number of matching entities across all pages.
        page: Current page number (1-based).
        limit: Maximum items per page.
        has_next: ``True`` if more pages follow.
    """

    items: tuple[OutputT, ...]
    total_count: int
    page: int
    limit: int
    has_next: bool


def build_page_result(
    items: list[OutputT],
    total_count: int,
    page_params: PageParams,
) -> PageResult[OutputT]:
    """Construct a ``PageResult`` from a list of items and pagination metadata.

    Args:
        items: Entity output structs for the current page.
        total_count: Total number of matching entities across all pages.
        page_params: The pagination parameters used for this query.

    Returns:
        A populated ``PageResult`` instance.
    """
    return PageResult(
        items=tuple(items),
        total_count=total_count,
        page=page_params.page,
        limit=page_params.limit,
        has_next=(page_params.offset + len(items)) < total_count,
    )
