from __future__ import annotations

from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    JSON,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY as PG_ARRAY
from sqlalchemy.dialects.postgresql import JSONB as PG_JSONB
from sqlalchemy.dialects.postgresql import TSVECTOR as PG_TSVECTOR
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, mapped_column, relationship

from loom.core.model.enums import Cardinality, ServerDefault
from loom.core.model.field import ColumnType, Field
from loom.core.model.introspection import (
    get_column_fields,
    get_relations,
    get_table_name,
)
from loom.core.model.relation import Relation

_SA_TYPE_MAP: dict[str, type] = {
    "String": String,
    "Integer": Integer,
    "BigInteger": BigInteger,
    "Float": Float,
    "Boolean": Boolean,
    "Text": Text,
    "JSON": JSON,
    "DateTime": DateTime,
    "Numeric": Numeric,
    "Postgres.JSONB": PG_JSONB,
    "Postgres.UUID": PG_UUID,
    "Postgres.TSVECTOR": PG_TSVECTOR,
}

_SERVER_DEFAULT_MAP = {
    ServerDefault.NOW: func.now,
}

_CARDINALITY_USELIST = {
    Cardinality.ONE_TO_ONE: False,
    Cardinality.MANY_TO_ONE: False,
    Cardinality.ONE_TO_MANY: True,
    Cardinality.MANY_TO_MANY: True,
}


class SABase(DeclarativeBase):
    """Shared declarative base for all compiled SQLAlchemy models."""


_registry: dict[type, type] = {}


def _build_sa_column_type(col_type: ColumnType) -> Any:
    if col_type.type_name == "Postgres.ARRAY":
        if len(col_type.args) != 1:
            raise ValueError("Postgres.ARRAY expects one inner ColumnType")
        inner = col_type.args[0]
        if not isinstance(inner, ColumnType):
            raise ValueError("Postgres.ARRAY inner type must be ColumnType")
        return PG_ARRAY(_build_sa_column_type(inner))

    sa_type_cls = _SA_TYPE_MAP.get(col_type.type_name)
    if sa_type_cls is None:
        raise ValueError(f"Unsupported column type: {col_type.type_name}")
    return sa_type_cls(*col_type.args, **col_type.kwargs)


def _build_mapped_column(field_info: Any) -> Any:
    col_type = _build_sa_column_type(field_info.column_type)
    field: Field = field_info.field

    kwargs: dict[str, Any] = {
        "nullable": field.nullable,
    }

    if field.primary_key:
        kwargs["primary_key"] = True
    if field.autoincrement:
        kwargs["autoincrement"] = True
    if field.unique:
        kwargs["unique"] = True
    if field.index:
        kwargs["index"] = True
    if field.default is not None:
        kwargs["default"] = field.default

    if field.server_default is not None:
        factory = _SERVER_DEFAULT_MAP.get(field.server_default)
        if factory is not None:
            kwargs["server_default"] = factory()

    if field.server_onupdate is not None and field.server_onupdate == "now":
        kwargs["server_onupdate"] = func.now()

    if field.foreign_key is not None:
        fk_kwargs: dict[str, Any] = {}
        if field.on_delete is not None:
            fk_kwargs["ondelete"] = field.on_delete.value
        return mapped_column(ForeignKey(field.foreign_key, **fk_kwargs), type_=col_type, **kwargs)

    return mapped_column(col_type, **kwargs)


def _resolve_fk_target_table(foreign_key: str) -> str:
    """Extract table name from a FK spec like 'products.id' -> 'products'."""
    return foreign_key.rsplit(".", 1)[0]


def _find_target_sa_class(target_table: str) -> type | None:
    """Find compiled SA class by table name."""
    for _struct_cls, sa_cls in _registry.items():
        if getattr(sa_cls, "__tablename__", None) == target_table:
            return sa_cls
    return None


def compile_model(struct_cls: type) -> type:
    """Compile a loom ``BaseModel`` Struct into a SQLAlchemy declarative class."""
    if struct_cls in _registry:
        return _registry[struct_cls]

    table_name = get_table_name(struct_cls)
    column_fields = get_column_fields(struct_cls)
    relations = get_relations(struct_cls)

    attrs: dict[str, Any] = {
        "__tablename__": table_name,
        "__struct_cls__": struct_cls,
    }

    for name, field_info in column_fields.items():
        attrs[name] = _build_mapped_column(field_info)

    # Register early so FK-target lookups can find this class
    sa_cls = type(struct_cls.__name__ + "SA", (SABase,), attrs)
    _registry[struct_cls] = sa_cls

    # Relationships are added after all column-based setup
    _pending_relations[struct_cls] = relations

    return sa_cls


# Deferred relationship queue — resolved by compile_all or configure_relationships
_pending_relations: dict[type, dict[str, Relation]] = {}


def _configure_relationships() -> None:
    """Resolve and attach deferred relationships to compiled SA classes."""
    for struct_cls, relations in list(_pending_relations.items()):
        if not relations:
            continue
        sa_cls = _registry[struct_cls]

        for rel_name, rel in relations.items():
            target_table = _resolve_fk_target_table(rel.foreign_key)

            # Find the SA model that owns the FK column
            # For ONE_TO_MANY: FK is on the target table, target references parent
            # For MANY_TO_ONE: FK is on this table
            # For MANY_TO_MANY: FK is on the secondary table
            if rel.cardinality == Cardinality.MANY_TO_MANY:
                target_sa = _find_target_sa_by_secondary(rel.secondary, rel.foreign_key)
            elif rel.cardinality in (Cardinality.ONE_TO_MANY, Cardinality.ONE_TO_ONE):
                target_sa = _find_target_sa_by_fk_column(rel.foreign_key)
            else:
                target_sa = _find_target_sa_class(target_table)

            if target_sa is None:
                continue

            kwargs: dict[str, Any] = {
                "lazy": "noload",
                "uselist": _CARDINALITY_USELIST.get(rel.cardinality, True),
                "info": {
                    "profiles": rel.profiles,
                    "depends_on": rel.depends_on,
                },
            }

            if rel.back_populates:
                kwargs["back_populates"] = rel.back_populates

            if rel.secondary:
                kwargs["secondary"] = _resolve_secondary_table(rel.secondary)

            sa_cls.__mapper__.add_property(  # type: ignore[attr-defined]
                rel_name,
                relationship(target_sa, **kwargs),
            )

    _pending_relations.clear()


def _find_target_sa_by_fk_column(foreign_key: str) -> type | None:
    """For ONE_TO_MANY: the FK column (e.g. 'product_id') lives on the target table.
    We search all compiled SA classes for one that has that column as a FK pointing to the parent.
    """
    # foreign_key in Relation is the column name, not fully qualified
    for _struct_cls, sa_cls in _registry.items():
        table = getattr(sa_cls, "__table__", None)
        if table is None:
            continue
        if foreign_key in table.c:
            return sa_cls
    return None


def _find_target_sa_by_secondary(secondary_name: str | None, foreign_key: str) -> type | None:
    """For MANY_TO_MANY: find the target class that is NOT the secondary table
    and is referenced by the secondary's FK columns.
    """
    if secondary_name is None:
        return None
    secondary_table = _resolve_secondary_table(secondary_name)
    if secondary_table is None:
        return None

    # Find all FK targets from the secondary table that aren't the owner
    fk_targets: set[str] = set()
    for col in secondary_table.columns:
        for fk in col.foreign_keys:
            fk_targets.add(fk.column.table.name)

    # The target is the one NOT containing the foreign_key column
    for _struct_cls, sa_cls in _registry.items():
        table_name = getattr(sa_cls, "__tablename__", None)
        if table_name in fk_targets:
            # Check this table doesn't have the FK column
            table = getattr(sa_cls, "__table__", None)
            if table is not None and foreign_key not in table.c:
                return sa_cls
    return None


def _resolve_secondary_table(name: str | None) -> Table | None:
    """Resolve a secondary table name to an actual SA Table."""
    if name is None:
        return None
    return SABase.metadata.tables.get(name)


def compile_all(*classes: type) -> None:
    """Batch-compile multiple model classes, then resolve deferred relationships."""
    for cls in classes:
        compile_model(cls)
    _configure_relationships()


def get_compiled(struct_cls: type) -> type | None:
    """Look up the compiled SA class for a given Struct model."""
    return _registry.get(struct_cls)


def get_metadata() -> MetaData:
    """Return the shared metadata for Alembic and table creation."""
    return SABase.metadata


def reset_registry() -> None:
    """Clear compiled models — primarily for testing."""
    _registry.clear()
    _pending_relations.clear()
    SABase.metadata.clear()
    SABase.registry.dispose()
