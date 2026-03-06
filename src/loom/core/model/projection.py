from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

PROJECTION_DEFAULT_MISSING = object()


@dataclass(frozen=True, slots=True)
class Projection:
    """Derived-field metadata assigned as a class attribute on a ``BaseModel``."""

    loader: Any
    profiles: tuple[str, ...] = ("default",)
    depends_on: tuple[str, ...] = ()
    default: Any = PROJECTION_DEFAULT_MISSING


def ProjectionField(
    *,
    loader: Any,
    profiles: tuple[str, ...] = ("default",),
    depends_on: tuple[str, ...] = (),
    default: Any = PROJECTION_DEFAULT_MISSING,
) -> Any:
    """Declare a projection field without triggering assignment type errors.

    Args:
        loader: Loader descriptor or instance responsible for computing the value.
        profiles: Profile names in which this projection is active.
        depends_on: Names of other projections or relation events this depends on.
        default: Fallback value when the loader returns no result for an entity.

    Returns:
        A :class:`Projection` instance cast to ``Any`` for clean class-body assignment.

    Example::

        count_reviews: int = ProjectionField(
            loader=CountLoader(model=ProductReview),
            profiles=("with_details",),
            default=0,
        )
    """
    return cast(
        Any,
        Projection(
            loader=loader,
            profiles=profiles,
            depends_on=depends_on,
            default=default,
        ),
    )


projection_field = ProjectionField
