from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast


@dataclass(frozen=True, slots=True)
class Projection:
    """Derived-field metadata assigned as a class attribute on a ``BaseModel``."""

    loader: Any
    profiles: tuple[str, ...] = ("default",)
    depends_on: tuple[str, ...] = ()
    default: Any = None


def ProjectionField(
    *,
    loader: Any,
    profiles: tuple[str, ...] = ("default",),
    depends_on: tuple[str, ...] = (),
    default: Any = None,
) -> Any:
    """Declare a projection field with normal typing (without assignment type errors)."""
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
