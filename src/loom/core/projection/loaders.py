from __future__ import annotations

import types
import typing
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


# ---------------------------------------------------------------------------
# Public loader descriptors — declared on model classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CountLoader:
    """Projection loader descriptor: counts related rows per parent entity.

    The compiler resolves the execution strategy at ``compile_all()`` time:
    memory-path when the relation is loaded in the active profile,
    SQL-path otherwise.

    Args:
        model: Target model class whose rows should be counted.
        via: Optional relation attribute name for disambiguation when
            multiple relations target the same model.

    Example::

        count_reviews: int = ProjectionField(
            loader=CountLoader(model=ProductReview),
            profiles=("with_details",),
            default=0,
        )
    """

    model: type
    via: str | None = None


@dataclass(frozen=True, slots=True)
class ExistsLoader:
    """Projection loader descriptor: checks if related rows exist per parent entity.

    Args:
        model: Target model class to check for existence.
        via: Optional relation attribute name for disambiguation.

    Example::

        has_reviews: bool = ProjectionField(
            loader=ExistsLoader(model=ProductReview),
            profiles=("with_details",),
            default=False,
        )
    """

    model: type
    via: str | None = None


@dataclass(frozen=True, slots=True)
class JoinFieldsLoader:
    """Projection loader descriptor: fetches selected columns from related rows.

    Args:
        model: Target model class to fetch columns from.
        value_columns: Column names to include in each result dict.
        via: Optional relation attribute name for disambiguation.

    Example::

        review_snippets: list[dict] = ProjectionField(
            loader=JoinFieldsLoader(model=ProductReview, value_columns=("id", "rating")),
            profiles=("with_details",),
            default=[],
        )
    """

    model: type
    value_columns: tuple[str, ...]
    via: str | None = None

    def __init__(
        self,
        *,
        model: type,
        value_columns: Sequence[str],
        via: str | None = None,
    ) -> None:
        object.__setattr__(self, "model", model)
        object.__setattr__(self, "value_columns", tuple(value_columns))
        object.__setattr__(self, "via", via)


# ---------------------------------------------------------------------------
# Internal memory-path loaders — synthesized at compile time
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _MemoryCountLoader:
    relation: str

    def load_from_object(
        self,
        obj: Any,
        context: Mapping[str, Any] | None = None,
    ) -> int:
        _ = context
        return len(_related_values(obj, self.relation))


@dataclass(frozen=True, slots=True)
class _MemoryExistsLoader:
    relation: str

    def load_from_object(
        self,
        obj: Any,
        context: Mapping[str, Any] | None = None,
    ) -> bool:
        _ = context
        return len(_related_values(obj, self.relation)) > 0


@dataclass(frozen=True, slots=True)
class _MemoryJoinFieldsLoader:
    relation: str
    value_columns: tuple[str, ...]

    def __init__(self, *, relation: str, value_columns: Sequence[str]) -> None:
        object.__setattr__(self, "relation", relation)
        object.__setattr__(self, "value_columns", tuple(value_columns))

    def load_from_object(
        self,
        obj: Any,
        context: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        _ = context
        rows = _related_values(obj, self.relation)
        return [
            {field_name: getattr(row, field_name, None) for field_name in self.value_columns}
            for row in rows
        ]


# ---------------------------------------------------------------------------
# Loader resolution utilities
# ---------------------------------------------------------------------------


def make_memory_loader(loader: Any, rel_name: str) -> Any:
    """Convert a public loader descriptor to its memory-path counterpart.

    Args:
        loader: A ``CountLoader``, ``ExistsLoader``, or ``JoinFieldsLoader`` descriptor.
        rel_name: The relation attribute name to read from the object.

    Returns:
        The corresponding ``_Memory*`` loader instance, or ``loader`` unchanged
        if it is not a recognised descriptor type.
    """
    if isinstance(loader, CountLoader):
        return _MemoryCountLoader(relation=rel_name)
    if isinstance(loader, ExistsLoader):
        return _MemoryExistsLoader(relation=rel_name)
    if isinstance(loader, JoinFieldsLoader):
        return _MemoryJoinFieldsLoader(relation=rel_name, value_columns=loader.value_columns)
    return loader


def find_relation_name_for_loader(loader: Any, parent_model: type) -> str | None:
    """Find the relation attribute on ``parent_model`` that matches ``loader.model``.

    If ``loader.via`` is set it is returned directly. Otherwise, the model's
    type hints are scanned for a relation attribute whose annotation resolves
    to ``loader.model`` (unwrapping ``list[X]``, ``X | UnsetType``, etc.).

    Args:
        loader: A ``CountLoader``, ``ExistsLoader``, or ``JoinFieldsLoader``.
        parent_model: The domain model class that owns the projection.

    Returns:
        The relation attribute name, or ``None`` if no match is found.
    """
    if not isinstance(loader, (CountLoader, ExistsLoader, JoinFieldsLoader)):
        return None
    if loader.via is not None:
        return loader.via

    from loom.core.model.introspection import get_relations

    relations = get_relations(parent_model)
    try:
        hints = typing.get_type_hints(parent_model)
    except Exception:
        return None

    target_model = loader.model
    for rel_name in relations:
        hint = hints.get(rel_name)
        if hint is None:
            continue
        if _extract_model_from_hint(hint) is target_model:
            return rel_name
    return None


def _extract_model_from_hint(hint: Any) -> type | None:
    """Unwrap list/Union/UnsetType annotations to extract the concrete model type."""
    # Python 3.10+ X | Y syntax (types.UnionType)
    if isinstance(hint, types.UnionType):
        for arg in hint.__args__:
            if arg is not type(None) and arg is not msgspec.UnsetType:
                result = _extract_model_from_hint(arg)
                if result is not None:
                    return result
        return None

    origin = getattr(hint, "__origin__", None)
    args: tuple[Any, ...] = getattr(hint, "__args__", ())

    # typing.Union[X, Y]
    if origin is typing.Union:
        for arg in args:
            if arg is not type(None) and arg is not msgspec.UnsetType:
                result = _extract_model_from_hint(arg)
                if result is not None:
                    return result
        return None

    # list[X]
    if origin is list and len(args) == 1:
        return _extract_model_from_hint(args[0])

    if isinstance(hint, type):
        return hint
    return None
