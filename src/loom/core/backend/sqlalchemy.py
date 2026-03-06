from __future__ import annotations

from typing import Any

import msgspec
from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
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

from loom.core.backend.core_model import CoreModel, CoreProfilePlan, CoreRelationStep
from loom.core.model.enums import Cardinality, ServerDefault, ServerOnUpdate
from loom.core.model.field import ColumnType, Field
from loom.core.model.introspection import (
    get_column_fields,
    get_id_attribute,
    get_projections,
    get_relations,
    get_table_name,
)
from loom.core.model.relation import Relation
from loom.core.projection.runtime import build_projection_plan

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
_table_registry: dict[str, type] = {}
_core_registry: dict[type, CoreModel] = {}


def _uses_now_onupdate(value: ServerOnUpdate | str | None) -> bool:
    if value is ServerOnUpdate.NOW:
        return True
    if isinstance(value, str):
        return value.strip().lower() == ServerOnUpdate.NOW.value
    return False


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


def _build_field_kwargs(field: Field) -> dict[str, Any]:
    kwargs: dict[str, Any] = {"nullable": field.nullable}
    if field.primary_key:
        kwargs["primary_key"] = True
    if field.autoincrement:
        kwargs["autoincrement"] = True
    if field.unique:
        kwargs["unique"] = True
    if field.index:
        kwargs["index"] = True
    if field.default not in (None, msgspec.UNSET):
        kwargs["default"] = field.default
    if field.server_default is not None:
        factory = _SERVER_DEFAULT_MAP.get(field.server_default)
        if factory is not None:
            kwargs["server_default"] = factory()
    if _uses_now_onupdate(field.server_onupdate):
        now_expr = func.now()
        kwargs["onupdate"] = now_expr
        kwargs["server_onupdate"] = now_expr
    return kwargs


def _build_mapped_column(field_info: Any) -> Any:
    col_type = _build_sa_column_type(field_info.column_type)
    field: Field = field_info.field
    kwargs = _build_field_kwargs(field)

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
    """Find compiled SA class by table name (O(1) via inverse index)."""
    return _table_registry.get(target_table)


# Deferred relationship queue — resolved by compile_all or configure_relationships
_pending_relations: dict[type, dict[str, Relation]] = {}


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
    _table_registry[table_name] = sa_cls

    # Relationships are added after all column-based setup
    _pending_relations[struct_cls] = relations

    return sa_cls


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
    """Batch-compile multiple model classes, resolve relationships, build Core artifacts."""
    for cls in classes:
        compile_model(cls)
    _configure_relationships()
    for cls in classes:
        _compile_core_model(cls)


def get_compiled(struct_cls: type) -> type | None:
    """Look up the compiled SA class for a given Struct model."""
    return _registry.get(struct_cls)


def get_compiled_core(struct_cls: type) -> CoreModel | None:
    """Look up the compiled :class:`~loom.core.backend.core_model.CoreModel` for a Struct model.

    Returns ``None`` if the model has not been compiled via :func:`compile_all`.

    Args:
        struct_cls: The ``BaseModel`` Struct class.

    Returns:
        :class:`CoreModel` instance with column expressions and read methods,
        or ``None``.
    """
    return _core_registry.get(struct_cls)


def get_metadata() -> MetaData:
    """Return the shared metadata for Alembic and table creation."""
    return SABase.metadata


def reset_registry() -> None:
    """Clear compiled models — primarily for testing."""
    _registry.clear()
    _table_registry.clear()
    _core_registry.clear()
    _pending_relations.clear()
    SABase.metadata.clear()
    SABase.registry.dispose()


# ---------------------------------------------------------------------------
# Core model compilation
# ---------------------------------------------------------------------------


def _compile_core_model(struct_cls: type) -> None:
    sa_cls = _registry.get(struct_cls)
    if sa_cls is None:
        return

    table: Any = sa_cls.__table__
    id_attr = get_id_attribute(struct_cls)
    column_fields = get_column_fields(struct_cls)
    relations = get_relations(struct_cls)
    projections = get_projections(struct_cls)

    all_columns = tuple(table.c[name] for name in column_fields if name in table.c)

    profiles = _collect_profiles(relations, projections)
    relation_steps = _compile_relation_steps(struct_cls, relations)
    profile_plans = _build_profile_plans(
        profiles, all_columns, id_attr, relations, relation_steps, projections
    )

    core_model = CoreModel(struct_cls, table, id_attr, profile_plans)
    for name, col in zip(column_fields, all_columns, strict=False):
        setattr(core_model, name, col)

    _core_registry[struct_cls] = core_model


def _collect_profiles(
    relations: dict[str, Any],
    projections: dict[str, Any],
) -> set[str]:
    profiles: set[str] = {"default"}
    for rel in relations.values():
        profiles.update(rel.profiles)
    for proj in projections.values():
        profiles.update(proj.profiles)
    return profiles


def _build_profile_plans(
    profiles: set[str],
    all_columns: tuple[Any, ...],
    id_attr: str,
    relations: dict[str, Any],
    relation_steps: dict[str, CoreRelationStep],
    projections: dict[str, Any],
) -> dict[str, CoreProfilePlan]:
    plans: dict[str, CoreProfilePlan] = {}
    for profile in profiles:
        profile_steps = tuple(
            relation_steps[name]
            for name, rel in relations.items()
            if name in relation_steps and profile in rel.profiles
        )
        profile_projections = {
            name: proj for name, proj in projections.items() if profile in proj.profiles
        }
        plans[profile] = CoreProfilePlan(
            columns=all_columns,
            relation_steps=profile_steps,
            projection_plan=build_projection_plan(profile_projections),
            id_attr=id_attr,
        )
    return plans


def _compile_relation_steps(
    struct_cls: type,
    relations: dict[str, Any],
) -> dict[str, CoreRelationStep]:
    steps: dict[str, CoreRelationStep] = {}
    for rel_name, rel in relations.items():
        step = _compile_relation_step(struct_cls, rel_name, rel)
        if step is not None:
            steps[rel_name] = step
    return steps


def _compile_relation_step(
    struct_cls: type,
    rel_name: str,
    rel: Relation,
) -> CoreRelationStep | None:
    if rel.cardinality in (Cardinality.ONE_TO_MANY, Cardinality.ONE_TO_ONE):
        return _compile_one_to_x_step(struct_cls, rel_name, rel)
    if rel.cardinality is Cardinality.MANY_TO_ONE:
        return _compile_many_to_one_step(struct_cls, rel_name, rel)
    if rel.cardinality is Cardinality.MANY_TO_MANY:
        return _compile_many_to_many_step(struct_cls, rel_name, rel)
    return None


def _compile_one_to_x_step(
    struct_cls: type,
    rel_name: str,
    rel: Relation,
) -> CoreRelationStep | None:
    target_sa = _find_target_sa_by_fk_column(rel.foreign_key)
    if target_sa is None:
        return None
    related_struct = getattr(target_sa, "__struct_cls__", None)
    if related_struct is None:
        return None
    target_table: Any = target_sa.__table__
    target_cols = _target_cols(related_struct, target_table)
    return CoreRelationStep(
        attr=rel_name,
        cardinality=rel.cardinality,
        target_table=target_table,
        target_cols=target_cols,
        related_struct=related_struct,
        owner_pk_col=get_id_attribute(struct_cls),
        fk_col=rel.foreign_key,
        pk_col=get_id_attribute(struct_cls),
    )


def _compile_many_to_one_step(
    struct_cls: type,
    rel_name: str,
    rel: Relation,
) -> CoreRelationStep | None:
    target_table_name, target_pk_name = rel.foreign_key.rsplit(".", 1)
    target_sa = _find_target_sa_class(target_table_name)
    if target_sa is None:
        return None
    related_struct = getattr(target_sa, "__struct_cls__", None)
    if related_struct is None:
        return None
    owner_fk_col = _find_owner_fk_col(struct_cls, rel.foreign_key)
    if owner_fk_col is None:
        return None
    target_table: Any = target_sa.__table__
    target_cols = _target_cols(related_struct, target_table)
    return CoreRelationStep(
        attr=rel_name,
        cardinality=rel.cardinality,
        target_table=target_table,
        target_cols=target_cols,
        related_struct=related_struct,
        owner_pk_col=get_id_attribute(struct_cls),
        fk_col=owner_fk_col,
        pk_col=target_pk_name,
    )


def _compile_many_to_many_step(
    struct_cls: type,
    rel_name: str,
    rel: Relation,
) -> CoreRelationStep | None:
    if rel.secondary is None:
        return None
    secondary_table: Any = SABase.metadata.tables.get(rel.secondary)
    if secondary_table is None:
        return None
    owner_table_name = get_table_name(struct_cls)
    secondary_owner_fk, secondary_target_fk, target_table_name = _inspect_secondary(
        secondary_table, owner_table_name
    )
    if secondary_owner_fk is None or secondary_target_fk is None or target_table_name is None:
        return None
    target_sa = _find_target_sa_class(target_table_name)
    if target_sa is None:
        return None
    related_struct = getattr(target_sa, "__struct_cls__", None)
    if related_struct is None:
        return None
    target_table: Any = target_sa.__table__
    target_cols = _target_cols(related_struct, target_table)
    return CoreRelationStep(
        attr=rel_name,
        cardinality=rel.cardinality,
        target_table=target_table,
        target_cols=target_cols,
        related_struct=related_struct,
        owner_pk_col=get_id_attribute(struct_cls),
        fk_col=secondary_owner_fk,
        pk_col=get_id_attribute(related_struct),
        secondary_table=secondary_table,
        secondary_target_fk=secondary_target_fk,
    )


def _inspect_secondary(
    secondary_table: Any,
    owner_table_name: str,
) -> tuple[str | None, str | None, str | None]:
    owner_fk: str | None = None
    target_fk: str | None = None
    target_table_name: str | None = None
    for col in secondary_table.columns:
        for fk in col.foreign_keys:
            if fk.column.table.name == owner_table_name:
                owner_fk = col.name
            else:
                target_fk = col.name
                target_table_name = fk.column.table.name
    return owner_fk, target_fk, target_table_name


def _target_cols(related_struct: type, target_table: Any) -> tuple[Any, ...]:
    col_fields = get_column_fields(related_struct)
    return tuple(target_table.c[name] for name in col_fields if name in target_table.c)


def _find_owner_fk_col(struct_cls: type, foreign_key: str) -> str | None:
    for name, col_info in get_column_fields(struct_cls).items():
        if col_info.field.foreign_key == foreign_key:
            return name
    return None
