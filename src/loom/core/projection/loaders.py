from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import msgspec

_MISSING = object()


def _related_values(obj: Any, relation: str) -> list[Any]:
    if hasattr(obj, "__dict__") and relation not in obj.__dict__:
        raise RuntimeError(
            f"Relation loader requires '{relation}' to be present in {type(obj).__name__}.__dict__"
        )
    related = getattr(obj, relation, _MISSING)
    if related is _MISSING:
        raise RuntimeError(
            f"Relation loader requires relation '{relation}' on {type(obj).__name__}"
        )
    if related is msgspec.UNSET:
        return []
    if related is None:
        return []
    if isinstance(related, list):
        return related
    return [related]


@dataclass(frozen=True, slots=True)
class RelationCountLoader:
    """Compute count from an already-loaded relation."""

    relation: str
    foreign_key: str | None = None

    def load_from_object(
        self,
        obj: Any,
        context: Mapping[str, Any] | None = None,
    ) -> int:
        _ = context
        return len(_related_values(obj, self.relation))


@dataclass(frozen=True, slots=True)
class RelationExistsLoader:
    """Compute existence flag from an already-loaded relation."""

    relation: str
    foreign_key: str | None = None

    def load_from_object(
        self,
        obj: Any,
        context: Mapping[str, Any] | None = None,
    ) -> bool:
        _ = context
        return len(_related_values(obj, self.relation)) > 0


@dataclass(frozen=True, slots=True)
class RelationJoinFieldsLoader:
    """Project selected fields from an already-loaded relation."""

    relation: str
    value_columns: tuple[str, ...]
    foreign_key: str | None

    def __init__(
        self,
        *,
        relation: str,
        value_columns: Sequence[str],
        foreign_key: str | None = None,
    ) -> None:
        object.__setattr__(self, "relation", relation)
        object.__setattr__(self, "value_columns", tuple(value_columns))
        object.__setattr__(self, "foreign_key", foreign_key)

    def load_from_object(
        self,
        obj: Any,
        context: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        _ = context
        rows = _related_values(obj, self.relation)
        payload: list[dict[str, Any]] = []
        for row in rows:
            payload.append(
                {field_name: getattr(row, field_name, None) for field_name in self.value_columns}
            )
        return payload
