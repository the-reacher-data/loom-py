from __future__ import annotations

from dataclasses import dataclass
from typing import Any, get_type_hints

from loom.core.model.field import ColumnType, Field
from loom.core.model.projection import Projection
from loom.core.model.relation import Relation


@dataclass(frozen=True, slots=True)
class ColumnFieldInfo:
    """Resolved metadata for a single column field."""

    name: str
    python_type: type
    column_type: ColumnType
    field: Field


def get_column_fields(cls: type) -> dict[str, ColumnFieldInfo]:
    """Extract column fields (those annotated with ``ColumnType``) from a model class."""
    hints = get_type_hints(cls, include_extras=True)
    result: dict[str, ColumnFieldInfo] = {}

    for name, annotation in hints.items():
        metadata = _extract_metadata(annotation)
        if not metadata:
            continue

        column_type: ColumnType | None = None
        field = Field()

        for entry in metadata:
            if isinstance(entry, ColumnType):
                column_type = entry
            elif isinstance(entry, Field):
                field = entry

        if column_type is None:
            continue

        python_type = _extract_origin_type(annotation)
        result[name] = ColumnFieldInfo(
            name=name,
            python_type=python_type,
            column_type=column_type,
            field=field,
        )
    return result


def get_relations(cls: type) -> dict[str, Relation]:
    """Return relations registered by ``LoomStructMeta``."""
    return dict(getattr(cls, "__loom_relations__", {}))


def get_projections(cls: type) -> dict[str, Projection]:
    """Return projections registered by ``LoomStructMeta``."""
    return dict(getattr(cls, "__loom_projections__", {}))


def get_id_attribute(cls: type) -> str:
    """Return the name of the primary key field."""
    for name, info in get_column_fields(cls).items():
        if info.field.primary_key:
            return name
    raise ValueError(f"No primary key field found on {cls.__name__}")


def get_table_name(cls: type) -> str:
    """Return the ``__tablename__`` declared on the model."""
    table = getattr(cls, "__tablename__", None)
    if table is None:
        raise ValueError(f"{cls.__name__} does not declare __tablename__")
    return table


def _extract_metadata(annotation: Any) -> tuple[Any, ...]:
    """Pull metadata entries from ``Annotated[T, ...]``."""
    return getattr(annotation, "__metadata__", ())


def _extract_origin_type(annotation: Any) -> type:
    """Return the base type from ``Annotated[T, ...]``."""
    origin = getattr(annotation, "__origin__", None)
    if origin is not None:
        args = getattr(annotation, "__args__", ())
        if args:
            return args[0]
    return annotation
