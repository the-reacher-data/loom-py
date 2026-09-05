from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, time
from decimal import Decimal
from types import UnionType
from typing import Any, ClassVar, Union, cast, get_args, get_origin, get_type_hints

import msgspec

from loom.core.model.field import ColumnFieldSpec, ColumnType, Field
from loom.core.model.projection import Projection
from loom.core.model.relation import Relation
from loom.core.model.types import JSON, Boolean, DateTime, Float, Integer, Numeric, String


@dataclass(frozen=True, slots=True)
class ColumnFieldInfo:
    """Resolved metadata for a single column field."""

    name: str
    python_type: type
    column_type: ColumnType
    field: Field


def _collect_inherited_dict_metadata(cls: type, attr: str) -> dict[str, Any]:
    """Merge dict metadata from the full MRO (base -> subclass)."""
    merged: dict[str, Any] = {}
    for current in reversed(cls.__mro__):
        raw = getattr(current, attr, None)
        if isinstance(raw, dict):
            merged.update(raw)
    return merged


def get_column_fields(cls: type) -> dict[str, ColumnFieldInfo]:
    """Extract column fields from a model class."""
    declared_columns = _collect_inherited_dict_metadata(cls, "__loom_columns__")
    hints = get_type_hints(cls, include_extras=True)
    non_columns = set(get_relations(cls)) | set(get_projections(cls))

    result: dict[str, ColumnFieldInfo] = {}
    for struct_field in msgspec.structs.fields(cls):
        name = struct_field.name
        annotation = hints.get(name, Any)
        if name in non_columns or _is_classvar(annotation):
            continue
        result[name] = _resolve_column_field(
            name,
            annotation,
            struct_default=struct_field.default,
            declared=declared_columns.get(name),
        )
    return result


def _resolve_column_field(
    name: str,
    annotation: Any,
    *,
    struct_default: Any,
    declared: ColumnFieldSpec | None,
) -> ColumnFieldInfo:
    """Build the column metadata for one field, declared or inferred."""
    python_type = _extract_origin_type(annotation)
    if declared is not None:
        return ColumnFieldInfo(
            name=name,
            python_type=python_type,
            column_type=declared.column_type
            or _infer_column_type(annotation, field=declared.field),
            field=_with_struct_default(declared.field, struct_default),
        )

    annotated_type, annotated_field = _extract_annotated_column(annotation)
    if annotated_type is not None:
        return ColumnFieldInfo(
            name=name,
            python_type=python_type,
            column_type=annotated_type,
            field=_with_struct_default(annotated_field, struct_default),
        )

    inferred_field = _with_struct_default(Field(), struct_default)
    return ColumnFieldInfo(
        name=name,
        python_type=python_type,
        column_type=_infer_column_type(annotation, field=inferred_field),
        field=inferred_field,
    )


def _extract_annotated_column(annotation: Any) -> tuple[ColumnType | None, Field]:
    """Read the ``ColumnType`` and ``Field`` carried by ``Annotated[T, ...]``."""
    column_type: ColumnType | None = None
    field = Field()
    for entry in _extract_metadata(annotation):
        if isinstance(entry, ColumnType):
            column_type = entry
        elif isinstance(entry, Field):
            field = entry
    return column_type, field


def _with_struct_default(field: Field, struct_default: Any) -> Field:
    if field.default is not msgspec.UNSET:
        return field
    if struct_default is msgspec.NODEFAULT:
        return field
    return cast(Field, replace(field, default=struct_default))  # type: ignore[redundant-cast]


def get_relations(cls: type) -> dict[str, Relation]:
    """Return relations registered by ``LoomStructMeta``."""
    return _collect_inherited_dict_metadata(cls, "__loom_relations__")


def get_projections(cls: type) -> dict[str, Projection]:
    """Return projections registered by ``LoomStructMeta``."""
    return _collect_inherited_dict_metadata(cls, "__loom_projections__")


def get_id_attribute(cls: type) -> str:
    """Return the name of the primary key field."""
    for name, info in get_column_fields(cls).items():
        if info.field.primary_key:
            return name
    raise ValueError(f"No primary key field found on {cls.__name__}")


def get_table_name(cls: type) -> str:
    """Return the ``__tablename__`` declared on the model."""
    table = getattr(cls, "__tablename__", None)
    if not isinstance(table, str):
        raise ValueError(f"{cls.__name__} does not declare __tablename__")
    return table


def _extract_metadata(annotation: Any) -> tuple[Any, ...]:
    """Pull metadata entries from ``Annotated[T, ...]``."""
    return getattr(annotation, "__metadata__", ())


# ---------------------------------------------------------------------------
# Shared annotation helpers — used across the model, backend, cache and
# projection layers, which all read the same annotation shapes.
# ---------------------------------------------------------------------------


def resolve_type_hints(obj: Any, *, include_extras: bool = False) -> dict[str, Any]:
    """Return the resolved annotations of *obj*, or ``{}`` when they cannot be resolved.

    An annotation naming a class that is not importable from the defining
    module — a forward reference to a model declared elsewhere, for instance —
    is not fatal for callers that read annotations as one source of evidence
    among several. They treat a missing hint as "unknown" and fall back to
    other signals, so an empty mapping is the useful answer here.

    Args:
        obj: Class, function, or module whose annotations should be resolved.
        include_extras: Keep ``Annotated[T, ...]`` metadata instead of stripping it.

    Returns:
        Mapping of attribute name to resolved annotation, empty when unresolvable.
    """
    try:
        return get_type_hints(obj, include_extras=include_extras)
    except Exception:
        return {}


def union_inner_args(hint: Any) -> tuple[Any, ...] | None:
    """Return the non-``None``, non-``UnsetType`` members of a Union annotation.

    Normalises both the ``X | Y`` and the ``Union[X, Y]`` spelling.

    Args:
        hint: Any annotation object.

    Returns:
        The meaningful union members, or ``None`` when *hint* is not a Union.
    """
    if isinstance(hint, UnionType):
        raw: tuple[Any, ...] = hint.__args__
    elif getattr(hint, "__origin__", None) is Union:
        raw = getattr(hint, "__args__", ())
    else:
        return None
    return tuple(a for a in raw if a is not type(None) and a is not msgspec.UnsetType)


def extract_model_from_hint(hint: Any) -> type | None:
    """Unwrap list, Union and ``UnsetType`` layers down to the concrete class.

    ``list[Note]``, ``Note | UnsetType`` and ``list[Note] | None`` all resolve
    to ``Note``. An annotation whose innermost element is not a class, such as
    ``list[dict[str, Any]]``, resolves to ``None``.

    Args:
        hint: Any annotation object.

    Returns:
        The wrapped class, or ``None`` when the annotation wraps no class.
    """
    union_args = union_inner_args(hint)
    if union_args is not None:
        for arg in union_args:
            result = extract_model_from_hint(arg)
            if result is not None:
                return result
        return None

    origin = getattr(hint, "__origin__", None)
    args: tuple[Any, ...] = getattr(hint, "__args__", ())

    if origin is list and len(args) == 1:
        return extract_model_from_hint(args[0])

    return hint if isinstance(hint, type) else None


def list_element_type(annotation: Any) -> type | None:
    """Return ``T`` for a ``list[T]`` annotation, or ``None`` when there is no list.

    Unlike :func:`extract_model_from_hint` a list layer is required, so a bare
    ``Note`` yields ``None``. The union arms produced when ``LoomStructMeta``
    widens a relation field to ``list[T] | UnsetType`` are unwrapped first.

    Args:
        annotation: Any annotation object.

    Returns:
        The list element class, or ``None`` when absent or not a class.
    """
    union_args = union_inner_args(annotation)
    if union_args is not None:
        for arg in union_args:
            result = list_element_type(arg)
            if result is not None:
                return result
        return None

    if get_origin(annotation) is list:
        args = get_args(annotation)
        if args and isinstance(args[0], type):
            return args[0]
    return None


def generic_type_arg(annotation: Any, origin: Any) -> type | None:
    """Return the single class argument of ``origin[X]``, such as ``X`` in ``RepoFor[X]``.

    Args:
        annotation: Any annotation object.
        origin: The generic origin the annotation is expected to parametrise.

    Returns:
        The class argument, or ``None`` when the origin differs, the annotation
        is not generic, or its argument is not a single class.
    """
    if get_origin(annotation) is not origin:
        return None
    args = get_args(annotation)
    if len(args) != 1 or not isinstance(args[0], type):
        return None
    return args[0]


def _extract_origin_type(annotation: Any) -> type[Any]:
    """Return the base type from ``Annotated[T, ...]``."""
    origin = getattr(annotation, "__origin__", None)
    if origin is not None:
        args = getattr(annotation, "__args__", ())
        if args:
            value = args[0]
            if isinstance(value, type):
                return value
            return object
    raw = _unwrap_optional(annotation)
    origin = get_origin(raw)
    if origin is not None:
        if isinstance(origin, type):
            return origin
        return object
    if isinstance(raw, type):
        return raw
    return object


def _unwrap_optional(annotation: Any) -> Any:
    origin = get_origin(annotation)
    if origin in (UnionType, Union):
        args = tuple(arg for arg in get_args(annotation) if arg is not type(None))
        if len(args) == 1:
            return args[0]
    return annotation


def _is_classvar(annotation: Any) -> bool:
    return get_origin(annotation) is ClassVar


_SCALAR_TYPE_MAP: dict[type, ColumnType] = {
    int: Integer,
    float: Float,
    bool: Boolean,
    datetime: DateTime(tz=True),
    Decimal: Numeric(),
}


def _infer_column_type(annotation: Any, *, field: Field) -> ColumnType:
    base = _unwrap_optional(annotation)
    if get_origin(base) in (list, tuple, set, dict):
        return JSON
    python_type = _extract_origin_type(base)
    if python_type is str:
        return String(field.length)
    if python_type in (date, time):
        return String(None)
    return _SCALAR_TYPE_MAP.get(python_type, JSON)
