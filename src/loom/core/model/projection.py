from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, cast

PROJECTION_DEFAULT_MISSING = object()


class ProjectionSource(StrEnum):
    BACKEND = "backend"
    PRELOADED = "preloaded"
    AUTO = "auto"


class ProjectionAutoPolicy(StrEnum):
    BACKEND_THEN_PRELOADED = "backend_then_preloaded"
    PRELOADED_THEN_BACKEND = "preloaded_then_backend"


@dataclass(frozen=True, slots=True)
class Projection:
    """Derived-field metadata assigned as a class attribute on a ``BaseModel``."""

    loader: Any
    source: ProjectionSource = ProjectionSource.AUTO
    auto_policy: ProjectionAutoPolicy = ProjectionAutoPolicy.BACKEND_THEN_PRELOADED
    profiles: tuple[str, ...] = ("default",)
    depends_on: tuple[str, ...] = ()
    default: Any = PROJECTION_DEFAULT_MISSING


def ProjectionField(
    *,
    loader: Any,
    source: ProjectionSource = ProjectionSource.AUTO,
    auto_policy: ProjectionAutoPolicy = ProjectionAutoPolicy.BACKEND_THEN_PRELOADED,
    profiles: tuple[str, ...] = ("default",),
    depends_on: tuple[str, ...] = (),
    default: Any = PROJECTION_DEFAULT_MISSING,
) -> Any:
    """Declare a projection field with normal typing (without assignment type errors)."""
    return cast(
        Any,
        Projection(
            loader=loader,
            source=source,
            auto_policy=auto_policy,
            profiles=profiles,
            depends_on=depends_on,
            default=default,
        ),
    )


projection_field = ProjectionField
