from __future__ import annotations

import types as _types_mod
import typing
from typing import Any, get_type_hints

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
from loom.core.projection.runtime import ProjectionStep, build_projection_plan_from_steps

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


_registry: dict[type, Any] = {}
_table_registry: dict[str, Any] = {}
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


def _find_target_sa_class(target_table: str) -> Any:
    """Find compiled SA class by table name (O(1) via inverse index)."""
    return _table_registry.get(target_table)


# Deferred relationship queue — resolved by compile_all or configure_relationships
_pending_relations: dict[type, dict[str, Relation]] = {}


def compile_model(struct_cls: type) -> Any:
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

        try:
            hints = get_type_hints(struct_cls)
        except Exception:
            hints = {}

        for rel_name, rel in relations.items():
            target_sa = _resolve_relation_target(rel, hints.get(rel_name))
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

            sa_cls.__mapper__.add_property(
                rel_name,
                relationship(target_sa, **kwargs),
            )

    _pending_relations.clear()


def _resolve_relation_target(rel: Relation, hint: Any) -> Any:
    """Return the SA class for the given relation, using the field annotation as primary source.

    Annotation-based lookup is exact and avoids ambiguity when multiple tables
    share the same FK column name.  Column-name and table-name scans are kept
    as fallbacks for relations without a resolvable annotation.
    """
    if rel.cardinality == Cardinality.MANY_TO_MANY:
        return _find_target_sa_by_secondary(rel.secondary, rel.foreign_key)

    target_model = _extract_model_from_annotation(hint)
    if target_model is not None:
        sa = _registry.get(target_model)
        if sa is not None:
            return sa

    if rel.cardinality in (Cardinality.ONE_TO_MANY, Cardinality.ONE_TO_ONE):
        return _find_target_sa_by_fk_column(rel.foreign_key)

    return _find_target_sa_class(_resolve_fk_target_table(rel.foreign_key))


def _extract_model_from_annotation(hint: Any) -> type | None:
    """Unwrap list/Union annotations to extract the concrete model type."""
    if isinstance(hint, _types_mod.UnionType):
        for arg in hint.__args__:
            if arg is not type(None) and arg is not msgspec.UnsetType:
                result = _extract_model_from_annotation(arg)
                if result is not None:
                    return result
        return None

    origin = getattr(hint, "__origin__", None)
    args: tuple[Any, ...] = getattr(hint, "__args__", ())

    if origin is typing.Union:
        for arg in args:
            if arg is not type(None) and arg is not msgspec.UnsetType:
                result = _extract_model_from_annotation(arg)
                if result is not None:
                    return result
        return None

    if origin is list and len(args) == 1:
        return _extract_model_from_annotation(args[0])

    if isinstance(hint, type):
        return hint
    return None


def _fk_col_name(foreign_key: str) -> str:
    """Extract the bare column name from a possibly fully-qualified 'table.column' FK reference.

    Both ``"record_id"`` and ``"bench_notes.record_id"`` return ``"record_id"``.
    """
    return foreign_key.rsplit(".", 1)[-1]


def _find_target_sa_by_fk_column(foreign_key: str) -> Any:
    """For ONE_TO_MANY: the FK column lives on the target table.

    Accepts both short (``"record_id"``) and fully-qualified
    (``"bench_notes.record_id"``) forms — the column name is normalised
    before the ``table.c`` lookup so either format finds the right class.
    """
    col_name = _fk_col_name(foreign_key)
    for _struct_cls, sa_cls in _registry.items():
        table = getattr(sa_cls, "__table__", None)
        if table is None:
            continue
        if col_name in table.c:
            return sa_cls
    return None


def _find_target_sa_by_secondary(secondary_name: str | None, foreign_key: str) -> Any:
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


def _resolve_loader_model(loader: Any) -> type | None:
    """Extract the concrete model type from a public loader descriptor.

    Handles both direct references (``CountLoader(model=Note)``) and
    lambda-wrapped forward references (``CountLoader(model=lambda: Note)``).
    Returns ``None`` for custom loaders or when the lambda cannot be called.
    """
    from loom.core.projection.loaders import CountLoader, ExistsLoader, JoinFieldsLoader

    if not isinstance(loader, (CountLoader, ExistsLoader, JoinFieldsLoader)):
        return None
    raw = loader.model
    if isinstance(raw, type):
        return raw
    if callable(raw):
        try:
            resolved = raw()
            return resolved if isinstance(resolved, type) else None
        except Exception:
            return None
    return None


def _collect_direct_deps(struct_cls: type) -> frozenset[type]:
    """Return all ``BaseModel`` types that *struct_cls* references directly.

    Scans two sources:

    * **Relation annotations** — ``reviews: list[ProductReview]`` yields
      ``ProductReview``.  The annotation is unwrapped through
      :func:`~loom.core.projection.loaders._extract_model_from_hint` so
      ``list[X]``, ``X | UnsetType``, etc. all resolve to ``X``.

    * **Projection loaders** — ``CountLoader(model=ProductReview)`` yields
      ``ProductReview``. Lambda-wrapped forward references are resolved by
      calling the callable.

    Pure ``dict`` annotations and non-``BaseModel`` types are ignored, so
    many-to-many relations typed as ``list[dict[str, Any]]`` produce no
    dependency.
    """
    import typing

    from loom.core.model.base import BaseModel
    from loom.core.projection.loaders import _extract_model_from_hint

    deps: set[type] = set()

    try:
        hints = typing.get_type_hints(struct_cls)
    except Exception:
        hints = {}

    for rel_name in get_relations(struct_cls):
        hint = hints.get(rel_name)
        if hint is None:
            continue
        model = _extract_model_from_hint(hint)
        if model is not None and isinstance(model, type) and issubclass(model, BaseModel):
            deps.add(model)

    for proj in get_projections(struct_cls).values():
        model = _resolve_loader_model(proj.loader)
        if model is not None and issubclass(model, BaseModel):
            deps.add(model)

    return frozenset(deps)


def _topological_sort(
    models: list[type],
    deps: dict[type, frozenset[type]],
) -> list[type]:
    """Kahn's algorithm: return *models* with dependency leaves first.

    If a cycle is detected (remaining nodes after BFS exhaustion), the
    cyclic models are appended in arbitrary order.  Circular references
    are valid because ``compile_model`` is idempotent and relationships
    are resolved after all models are registered.
    """
    from collections import deque

    model_set = set(models)

    # in_degree[m] = number of m's deps that are within our compilation set
    in_degree: dict[type, int] = dict.fromkeys(models, 0)
    # dependents[dep] = list of models that depend on dep
    dependents: dict[type, list[type]] = {m: [] for m in models}

    for m in models:
        for dep in deps.get(m, frozenset()):
            if dep in model_set:
                in_degree[m] += 1
                dependents[dep].append(m)

    queue: deque[type] = deque(m for m in models if in_degree[m] == 0)
    result: list[type] = []

    while queue:
        node = queue.popleft()
        result.append(node)
        for dependent in dependents[node]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    # Cyclic nodes: append in original order
    processed = set(result)
    result.extend(m for m in models if m not in processed)
    return result


def _resolve_compile_closure(*roots: type) -> tuple[type, ...]:
    """BFS from *roots* to collect all transitive model dependencies.

    Follows relation annotations and projection loader ``model=`` references
    recursively.  Returns all discovered models in topological order
    (dependencies before dependants) so that ``compile_model`` sees each
    child model before its parent when resolving FK targets.

    Args:
        *roots: Explicitly requested model classes.

    Returns:
        Tuple of all models (roots + transitive deps) in compilation order.
    """
    from collections import deque

    seen: set[type] = set()
    all_deps: dict[type, frozenset[type]] = {}
    queue: deque[type] = deque(roots)

    while queue:
        cls = queue.popleft()
        if cls in seen:
            continue
        seen.add(cls)
        direct = _collect_direct_deps(cls)
        all_deps[cls] = direct
        for dep in direct:
            if dep not in seen:
                queue.append(dep)

    ordered = _topological_sort(list(all_deps.keys()), all_deps)
    return tuple(ordered)


def compile_all(*classes: type) -> None:
    """Batch-compile multiple model classes, resolve relationships, build Core artifacts.

    Automatically discovers and compiles all transitive model dependencies
    found via relation type annotations and projection loader ``model=``
    references.  Passing only the root models is sufficient — related models
    do not need to be listed explicitly.

    Compilation order is topological: child models (dependency leaves) are
    compiled before their parents so FK lookups during relationship
    resolution always find the target SA class in the registry.

    Args:
        *classes: Root model classes to compile.  Transitive dependencies
            are resolved automatically.

    Example::

        # ProductReview is compiled automatically because Product.reviews
        # is annotated as list[ProductReview].
        compile_all(Product)
    """
    ordered = _resolve_compile_closure(*classes)
    for cls in ordered:
        compile_model(cls)
    _configure_relationships()
    for cls in ordered:
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
        profiles, all_columns, id_attr, relations, relation_steps, projections, struct_cls
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
    struct_cls: type,
) -> dict[str, CoreProfilePlan]:
    plans: dict[str, CoreProfilePlan] = {}
    for profile in profiles:
        profile_steps = tuple(
            relation_steps[name]
            for name, rel in relations.items()
            if name in relation_steps and profile in rel.profiles
        )
        profile_relation_names = frozenset(step.attr for step in profile_steps)
        profile_projections = {
            name: proj for name, proj in projections.items() if profile in proj.profiles
        }
        resolved_steps = _resolve_projection_steps(
            profile_projections, profile_relation_names, relation_steps, struct_cls
        )
        plans[profile] = CoreProfilePlan(
            columns=all_columns,
            relation_steps=profile_steps,
            projection_plan=build_projection_plan_from_steps(resolved_steps),
            id_attr=id_attr,
        )
    return plans


def _resolve_projection_steps(
    projections: dict[str, Any],
    profile_relation_names: frozenset[str],
    all_relation_steps: dict[str, CoreRelationStep],
    struct_cls: type,
) -> dict[str, ProjectionStep]:
    """Resolve each projection to a ``ProjectionStep`` with the right loader strategy.

    For ``CountLoader``, ``ExistsLoader``, and ``JoinFieldsLoader`` descriptors the
    compiler picks between memory-path (when the target relation is loaded in this
    profile) and SQL-path (when it is not).  Custom loaders are auto-detected via
    capability inspection (``load_from_object`` vs ``load_many``).

    Args:
        projections: Profile-filtered projection metadata.
        profile_relation_names: Relation attribute names loaded in this profile.
        all_relation_steps: Compiled relation steps for all relations on the model.

    Returns:
        Mapping of field name to resolved :class:`ProjectionStep`.

    Raises:
        ValueError: If a descriptor loader cannot be matched to a relation step.
    """
    import inspect as _inspect

    from loom.core.projection.loaders import (
        CountLoader,
        ExistsLoader,
        JoinFieldsLoader,
        find_relation_name_for_loader,
        make_memory_loader,
    )
    from loom.core.repository.sqlalchemy.loaders import make_sql_loader

    steps: dict[str, ProjectionStep] = {}
    for name, proj in projections.items():
        loader = proj.loader
        if isinstance(loader, (CountLoader, ExistsLoader, JoinFieldsLoader)):
            rel_step = _find_relation_for_loader(loader, all_relation_steps)
            if rel_step is None:
                # Related model not compiled — fall back to memory path via type hints.
                rel_name = find_relation_name_for_loader(loader, struct_cls)
                if rel_name is None:
                    raise ValueError(
                        f"Projection '{name}': no relation found for "
                        f"{type(loader).__name__}(model={loader.model.__name__}) "
                        f"on {struct_cls.__name__}. "
                        "Ensure the model has a relation typed with the target class."
                    )
                resolved_loader = make_memory_loader(loader, rel_name)
                prefer_memory = True
            elif rel_step.attr in profile_relation_names:
                resolved_loader = make_memory_loader(loader, rel_step.attr)
                prefer_memory = True
            else:
                resolved_loader = make_sql_loader(loader, rel_step)
                prefer_memory = False
        else:

            def _has_cap(obj: Any, attr: str) -> bool:
                try:
                    candidate = _inspect.getattr_static(obj, attr)
                except AttributeError:
                    return False
                return callable(candidate)

            prefer_memory = _has_cap(loader, "load_from_object")
            resolved_loader = loader

        steps[name] = ProjectionStep(
            name=name,
            projection=proj,
            prefer_memory=prefer_memory,
            loader=resolved_loader,
        )
    return steps


def _find_relation_for_loader(
    loader: Any,
    all_relation_steps: dict[str, CoreRelationStep],
) -> CoreRelationStep | None:
    """Find the compiled relation step matching a public loader descriptor.

    Uses ``loader.via`` if provided, otherwise matches by ``related_struct``.

    Args:
        loader: A ``CountLoader``, ``ExistsLoader``, or ``JoinFieldsLoader``.
        all_relation_steps: All compiled relation steps for the parent model.

    Returns:
        Matching :class:`CoreRelationStep`, or ``None`` if not found.
    """
    via = getattr(loader, "via", None)
    if via is not None:
        return all_relation_steps.get(via)

    target_model = loader.model
    for step in all_relation_steps.values():
        if step.related_struct is target_model:
            return step
    return None


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
    try:
        hint = get_type_hints(struct_cls).get(rel_name)
    except Exception:
        hint = None
    target_sa = _resolve_relation_target(rel, hint)
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
        fk_col=_fk_col_name(rel.foreign_key),
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
