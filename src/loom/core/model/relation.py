from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, cast

from loom.core.model.enums import Cardinality, OnDelete, OnUpdate


@dataclass(frozen=True, slots=True)
class Relation:
    """Relationship metadata assigned as a class attribute on a ``BaseModel``."""

    foreign_key: str
    cardinality: Cardinality
    secondary: str | None = None
    back_populates: str | None = None
    on_delete: OnDelete | None = None
    on_update: OnUpdate | None = None
    profiles: tuple[str, ...] = ("default",)
    depends_on: tuple[str, ...] = ()
    default: Any = field(default=None)


def RelationField(
    *,
    foreign_key: str,
    cardinality: Cardinality,
    secondary: str | None = None,
    back_populates: str | None = None,
    on_delete: OnDelete | None = None,
    on_update: OnUpdate | None = None,
    profiles: tuple[str, ...] = ("default",),
    depends_on: tuple[str, ...] = (),
    default: Any = None,
) -> Any:
    """Declare a relation field with normal typing (without assignment type errors)."""
    return cast(
        Any,
        Relation(
            foreign_key=foreign_key,
            cardinality=cardinality,
            secondary=secondary,
            back_populates=back_populates,
            on_delete=on_delete,
            on_update=on_update,
            profiles=profiles,
            depends_on=depends_on,
            default=default,
        ),
    )


relation_field = RelationField
