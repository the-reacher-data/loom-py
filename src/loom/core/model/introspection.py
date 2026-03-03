from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, time
from decimal import Decimal
from types import UnionType
from typing import Any, ClassVar, Union, get_args, get_origin, get_type_hints

import msgspec

from loom.core.model.field import ColumnType, Field
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
    struct_fields = {field.name: field for field in msgspec.structs.fields(cls)}
    relations = set(get_relations(cls))
    projections = set(get_projections(cls))
    result: dict[str, ColumnFieldInfo] = {}

    for name, struct_field in struct_fields.items():
        if name in relations or name in projections:
            continue
        annotation = hints.get(name, Any)
        if _is_classvar(annotation):
            continue

        declared = declared_columns.get(name)
        if declared is not None:
            field = declared.field
            column_type = declared.column_type or _infer_column_type(annotation, field=field)
            result[name] = ColumnFieldInfo(
                name=name,
                python_type=_extract_origin_type(annotation),
                column_type=column_type,
                field=_with_struct_default(field, struct_field.default),
            )
            continue

        metadata = _extract_metadata(annotation)
        if metadata:
            annotated_column_type: ColumnType | None = None
            field = Field()

            for entry in metadata:
                if isinstance(entry, ColumnType):
                    annotated_column_type = entry
                elif isinstance(entry, Field):
                    field = entry
            if annotated_column_type is not None:
                result[name] = ColumnFieldInfo(
                    name=name,
                    python_type=_extract_origin_type(annotation),
                    column_type=annotated_column_type,
                    field=_with_struct_default(field, struct_field.default),
                )
                continue

        inferred_field = _with_struct_default(Field(), struct_field.default)
        result[name] = ColumnFieldInfo(
            name=name,
            python_type=_extract_origin_type(annotation),
            column_type=_infer_column_type(annotation, field=inferred_field),
            field=inferred_field,
        )
    return result


def _with_struct_default(field: Field, struct_default: Any) -> Field:
    if field.default is not msgspec.UNSET:
        return field
    if struct_default is msgspec.NODEFAULT:
        return field
    # Explicit annotation helps static analysers (e.g. Sonar) infer the
    # concrete return type; dataclasses.replace() is generically typed as _T.
    result: Field = replace(field, default=struct_default)
    return result


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
