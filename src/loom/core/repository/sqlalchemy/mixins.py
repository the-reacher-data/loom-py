from __future__ import annotations

import inspect as py_inspect
from contextlib import AbstractAsyncContextManager
from typing import Any, Generic, cast

import msgspec
from sqlalchemy import exists, func, inspect, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.core.backend.sqlalchemy import get_compiled_core
from loom.core.model.introspection import (
    get_column_fields,
    get_id_attribute,
    get_projections,
    get_relations,
    get_table_name,
)
from loom.core.model.projection import ProjectionSource
from loom.core.projection.runtime import build_projection_plan, execute_projection_plan
from loom.core.repository.abc import (
    CursorResult,
    FilterParams,
    IdT,
    OutputT,
    PageParams,
    PageResult,
    PaginationMode,
    QuerySpec,
    build_page_result,
)
from loom.core.repository.mutation import MutationEvent
from loom.core.repository.sqlalchemy.integrity import handle_integrity_errors
from loom.core.repository.sqlalchemy.query_compiler.compiler import QuerySpecCompiler
from loom.core.repository.sqlalchemy.query_compiler.cursor import extract_next_cursor
from loom.core.repository.sqlalchemy.transactional import record_mutation

_SENTINEL = object()
_TOTAL_COUNT_ALIAS = "__loom_total_count"


def _projected_relation_name(projection: Any) -> str | None:
    loader = projection.loader
    if projection.source is ProjectionSource.BACKEND:
        return None

    has_memory_loader = _has_declared_callable(loader, "load_from_object")
    if not has_memory_loader:
        return None

    relation_name = getattr(loader, "relation", None)
    if not isinstance(relation_name, str) or not relation_name:
        return None
    return relation_name


def _validate_projection_relation(
    *,
    model_name: str,
    projection_name: str,
    projection_profiles: tuple[str, ...],
    relation_name: str,
    relations: dict[str, Any],
) -> None:
    if relation_name not in relations:
        raise ValueError(
            f"'{model_name}.{projection_name}': relation-based loader references "
            f"relation '{relation_name}' which does not exist on this model."
        )

    unsupported_profiles = frozenset(projection_profiles) - frozenset(
        relations[relation_name].profiles
    )
    if not unsupported_profiles:
        return

    raise ValueError(
        f"'{model_name}.{projection_name}': "
        f"Relation loader(relation='{relation_name}') "
        f"is active in profiles {sorted(unsupported_profiles)} where '{relation_name}' is not "
        f"loaded. Add {sorted(unsupported_profiles)} to '{relation_name}'.profiles or restrict "
        f"'{projection_name}'.profiles to {sorted(relations[relation_name].profiles)}."
    )


def _has_declared_callable(obj: Any, attr_name: str) -> bool:
    try:
        candidate = py_inspect.getattr_static(obj, attr_name)
    except AttributeError:
        return False
    return callable(candidate)


class SQLAlchemyContextMixin(Generic[OutputT, IdT]):
    """Shared context and helper methods for all SQLAlchemy repository mixins."""

    model: type[Any]
    _sa_model: type[Any] | None = None
    _id_attr: str | None = None
    _column_field_names: frozenset[str] | None = None
    # Pre-computed at init to avoid per-request/per-object reflection.
    _output_column_keys: tuple[str, ...] | None = None
    _all_sa_column_keys: tuple[str, ...] | None = None
    _output_sa_columns: tuple[Any, ...] | None = None
    _core_model: Any | None = None
    _relations_cache: dict[str, Any] | None = None
    _projections_cache: dict[str, Any] | None = None
    _projection_plan_cache: dict[str, Any] | None = None

    def _init_struct_model(self) -> None:
        """Resolve SA model and metadata from a Struct-based model definition."""
        from loom.core.backend.sqlalchemy import get_compiled

        sa = get_compiled(self.model)
        if sa is None:
            raise RuntimeError(
                f"Model {self.model.__name__} has not been compiled. "
                f"Call compile_model() or compile_all() before creating a repository."
            )
        self._sa_model = sa
        self._id_attr = get_id_attribute(self.model)
        column_field_names = frozenset(get_column_fields(self.model).keys())
        self._column_field_names = column_field_names
        self._relations_cache = get_relations(self.model)
        self._projections_cache = get_projections(self.model)
        self._projection_plan_cache = {}
        self._core_model = get_compiled_core(self.model)
        if self._core_model is None:
            raise RuntimeError(
                f"Model {self.model.__name__} core read artifact was not compiled. "
                "Call compile_all() before creating a repository."
            )

        sa_mapper: Any = inspect(sa).mapper
        self._output_column_keys = tuple(
            col.key for col in sa_mapper.column_attrs if col.key in column_field_names
        )
        self._all_sa_column_keys = tuple(col.key for col in sa_mapper.column_attrs)
        self._output_sa_columns = tuple(getattr(sa, key) for key in self._output_column_keys)
        self._validate_computed_relation_loaders()

    def _validate_computed_relation_loaders(self) -> None:
        """Validate relation-based projection loaders at repository init time.

        Ensures every ``PRELOADED`` projection backed by a relation loader
        references an existing relation whose profiles cover all projection profiles.

        Raises:
            ValueError: If the referenced relation does not exist on the model,
                or if a projection is active in profiles where the relation is not loaded.
        """
        projections = self._projections_cache or {}
        relations = self._relations_cache or {}
        model_name = self.model.__name__

        for proj_name, proj in projections.items():
            relation_name = _projected_relation_name(proj)
            if relation_name is None:
                continue
            _validate_projection_relation(
                model_name=model_name,
                projection_name=proj_name,
                projection_profiles=proj.profiles,
                relation_name=relation_name,
                relations=relations,
            )

    @property
    def _effective_sa_model(self) -> type[Any]:
        if self._sa_model is not None:
            return self._sa_model
        return self.model

    @property
    def _effective_id_attribute(self) -> str:
        if self._id_attr is not None:
            return self._id_attr
        return "id"

    @property
    def _effective_core_model(self) -> Any:
        if self._core_model is not None:
            return self._core_model
        raise RuntimeError("Core model metadata is not initialized")

    def create_object(self, data: msgspec.Struct) -> Any:
        """Create a model instance from a struct without persisting it."""
        return self._effective_sa_model(**self._serialize_input(data))

    @property
    def entity_name(self) -> str:
        """Return normalized entity name for mutation tracking."""
        return get_table_name(self.model)

    def _id_column(self) -> Any:
        return getattr(self._effective_sa_model, self._effective_id_attribute)

    def _serialize_input(self, data: msgspec.Struct | dict[str, Any]) -> dict[str, Any]:
        if isinstance(data, msgspec.Struct):
            builtins = msgspec.to_builtins(data)
            if not isinstance(builtins, dict):
                raise TypeError("Struct payload must serialize to dict")
            return _to_internal_field_names(type(data), builtins)
        return data

    def _column_for_field(self, field: str) -> Any:
        """Resolve a model column attribute by field name.

        Raises:
            ValueError: If ``field`` does not exist on the SQLAlchemy model.
        """
        sa_model = self._effective_sa_model
        if not hasattr(sa_model, field):
            raise ValueError(f"Unknown field '{field}' for model '{self.model.__name__}'")
        return getattr(sa_model, field)

    def _apply_offset_page_filters(
        self,
        items_stmt: Any,
        filter_params: FilterParams | None,
    ) -> Any:
        """Apply page-mode filters to an offset items statement."""
        if not filter_params or not filter_params.filters:
            return items_stmt
        return items_stmt.filter_by(**filter_params.filters)

    def _to_output(
        self,
        obj: Any,
        profile: str = "default",
        projection_values: dict[str, Any] | None = None,
    ) -> Any:
        """Convert an SA ORM object to the Struct model."""
        output_keys = self._output_column_keys
        if output_keys is not None:
            kwargs: dict[str, Any] = {key: getattr(obj, key) for key in output_keys}
        else:
            mapper = inspect(obj).mapper
            column_names = self._column_field_names or frozenset()
            kwargs = {
                col.key: getattr(obj, col.key)
                for col in mapper.column_attrs
                if col.key in column_names
            }

        relations = (
            self._relations_cache
            if self._relations_cache is not None
            else get_relations(self.model)
        )
        for rel_name, rel in relations.items():
            if profile in rel.profiles and rel_name in obj.__dict__:
                kwargs[rel_name] = self._serialize_related(getattr(obj, rel_name))

        if projection_values:
            kwargs.update(projection_values)

        return self.model(**kwargs)

    def _serialize_related(self, value: Any) -> Any:
        """Recursively serialize ORM relationship values to dicts/lists."""
        if value is None:
            return None
        if isinstance(value, list):
            return [self._serialize_related(item) for item in value]
        if hasattr(value, "__mapper__"):
            rel_mapper = inspect(value).mapper
            return {col.key: getattr(value, col.key) for col in rel_mapper.column_attrs}
        return value

    def to_output_from_payload(self, payload: dict[str, Any]) -> Any:
        """Build output from a cached dict payload."""
        return msgspec.convert(dict(payload), type=self.model)

    def _mutation_tags(self, obj: Any) -> frozenset[str]:
        table_name = get_table_name(self.model)
        obj_id = getattr(obj, self._effective_id_attribute, None)
        tags: set[str] = {table_name, f"{table_name}:list"}
        if obj_id is not None:
            tags.add(f"{table_name}:id:{obj_id}")

        column_keys = self._all_sa_column_keys
        if column_keys is None:
            column_keys = tuple(col.key for col in inspect(obj).mapper.column_attrs)
        for key in column_keys:
            value = getattr(obj, key, None)
            if value is not None:
                tags.add(f"{table_name}:{key}:{value}")
        return frozenset(tags)

    def _projections_for_profile(self, profile: str) -> dict[str, Any]:
        """Return projections active for the given profile."""
        struct_projections = (
            self._projections_cache
            if self._projections_cache is not None
            else get_projections(self.model)
        )
        return {name: proj for name, proj in struct_projections.items() if profile in proj.profiles}

    async def _collect_projection_values(
        self,
        scoped_session: AsyncSession,
        objs: list[Any],
        profile: str,
    ) -> dict[int, dict[str, Any]]:
        """Batch-load projection values for a list of objects."""
        if not objs:
            return {}
        projections = self._projections_for_profile(profile)
        if not projections:
            return {}

        plan_cache = self._projection_plan_cache
        if plan_cache is None:
            plan_cache = {}
            self._projection_plan_cache = plan_cache

        plan = plan_cache.get(profile)
        if plan is None:
            plan = build_projection_plan(projections)
            plan_cache[profile] = plan

        return await execute_projection_plan(
            plan,
            objs=objs,
            id_attr=self._effective_id_attribute,
            backend_context=scoped_session,
        )

    def _session_scope(
        self, session: AsyncSession | None = None
    ) -> AbstractAsyncContextManager[AsyncSession]:
        raise NotImplementedError


class SQLAlchemyCreateMixin(SQLAlchemyContextMixin[OutputT, IdT], Generic[OutputT, IdT]):
    """Mixin providing the ``create`` operation for SQLAlchemy repositories."""

    @handle_integrity_errors
    async def create(self, data: msgspec.Struct) -> OutputT:
        """Persist one entity and return its output struct."""
        serialized = self._serialize_input(data)
        async with self._session_scope() as scoped_session:
            sa_model = self._effective_sa_model
            obj = sa_model(**serialized)
            scoped_session.add(obj)
            await scoped_session.flush()
            await scoped_session.refresh(obj)

            id_attr = self._effective_id_attribute
            obj_id = getattr(obj, id_attr, None)
            record_mutation(
                MutationEvent(
                    entity=self.entity_name,
                    op="create",
                    ids=(obj_id,),
                    changed_fields=frozenset(serialized.keys()),
                    tags=self._mutation_tags(obj),
                )
            )
            return cast(OutputT, self._to_output(obj))


class SQLAlchemyReadMixin(SQLAlchemyContextMixin[OutputT, IdT], Generic[OutputT, IdT]):
    """Read operations for SQLAlchemy repositories.

    Class attributes:
        allowed_filter_fields: Whitelist of field names permitted in
            :meth:`list_with_query` filters. An empty frozenset allows all.
    """

    allowed_filter_fields: frozenset[str] = frozenset()

    async def get_by_id(
        self,
        obj_id: IdT,
        profile: str = "default",
    ) -> OutputT | None:
        """Fetch one entity by id with optional profile-aware loading."""
        async with self._session_scope() as scoped_session:
            core_model = self._effective_core_model
            stmt = core_model.select(profile).where(self._id_column() == obj_id).limit(1)
            loaded = await core_model.fetch_one(scoped_session, stmt, profile=profile)
            return cast(OutputT | None, loaded)

    async def get_by(
        self,
        field: str,
        value: Any,
        profile: str = "default",
    ) -> OutputT | None:
        """Fetch one entity by equality over an arbitrary column field."""
        async with self._session_scope() as scoped_session:
            core_model = self._effective_core_model
            stmt = core_model.select(profile).where(self._column_for_field(field) == value).limit(1)
            loaded = await core_model.fetch_one(scoped_session, stmt, profile=profile)
            return cast(OutputT | None, loaded)

    async def list_paginated(
        self,
        page_params: PageParams,
        filter_params: FilterParams | None = None,
        profile: str = "default",
    ) -> PageResult[OutputT]:
        """Fetch a paginated entity list and total count."""
        async with self._session_scope() as scoped_session:
            core_model = self._effective_core_model
            stmt = core_model.select(profile).order_by(self._id_column())
            stmt = self._apply_offset_page_filters(stmt, filter_params)

            paged_stmt = stmt.offset(page_params.offset).limit(page_params.limit)
            paged_stmt = paged_stmt.add_columns(
                func.count().over().label(_TOTAL_COUNT_ALIAS)
            )
            loaded_items, total_count = await core_model.fetch_all_with_total(
                scoped_session,
                paged_stmt,
                profile=profile,
                total_alias=_TOTAL_COUNT_ALIAS,
            )
            items = cast(list[OutputT], loaded_items)
            return build_page_result(items, total_count, page_params)

    async def list_with_query(
        self,
        query: QuerySpec,
        profile: str = "default",
    ) -> PageResult[OutputT] | CursorResult[OutputT]:
        """Fetch entities using a structured
        :class:`~loom.core.repository.abc.query.QuerySpec`.

        Supports offset and cursor pagination, structured filters, and
        explicit sort directives.

        Args:
            query: Structured query specification.
            profile: Loading profile name for eager-load options.

        Returns:
            :class:`~loom.core.repository.abc.query.PageResult` for offset
            queries, :class:`~loom.core.repository.abc.query.CursorResult`
            for cursor queries.
        """
        async with self._session_scope() as scoped_session:
            sa_model = self._effective_sa_model
            id_col = self._id_column()
            compiler = QuerySpecCompiler(sa_model, id_col, self.allowed_filter_fields)
            core_model = self._effective_core_model

            if query.pagination == PaginationMode.CURSOR:
                return await self._list_with_query_cursor(
                    scoped_session,
                    query=query,
                    profile=profile,
                    compiler=compiler,
                    core_model=core_model,
                )

            return await self._list_with_query_offset(
                scoped_session,
                query=query,
                profile=profile,
                compiler=compiler,
                core_model=core_model,
            )

    async def _list_with_query_cursor(
        self,
        scoped_session: AsyncSession,
        *,
        query: QuerySpec,
        profile: str,
        compiler: QuerySpecCompiler,
        core_model: Any,
    ) -> CursorResult[OutputT]:
        cursor_stmt, cursor_field = compiler.compile_cursor(
            query,
            base_stmt=core_model.select(profile),
        )
        loaded = await core_model.fetch_all(scoped_session, cursor_stmt, profile=profile)
        items, next_cursor, has_next = extract_next_cursor(
            cast(list[Any], loaded),
            cursor_field,
            query.limit,
        )
        return CursorResult(
            items=tuple(cast(list[OutputT], items)),
            next_cursor=next_cursor,
            has_next=has_next,
        )

    async def _list_with_query_offset(
        self,
        scoped_session: AsyncSession,
        *,
        query: QuerySpec,
        profile: str,
        compiler: QuerySpecCompiler,
        core_model: Any,
    ) -> PageResult[OutputT]:
        offset_stmt = compiler.compile_offset(
            query,
            base_stmt=core_model.select(profile),
        )
        offset_stmt = offset_stmt.add_columns(
            func.count().over().label(_TOTAL_COUNT_ALIAS)
        )
        loaded_items, total_count = await core_model.fetch_all_with_total(
            scoped_session,
            offset_stmt,
            profile=profile,
            total_alias=_TOTAL_COUNT_ALIAS,
        )
        items = cast(list[OutputT], loaded_items)
        page_params = PageParams(page=query.page, limit=query.limit)
        return build_page_result(items, total_count or 0, page_params)

    async def exists(self, obj_id: IdT) -> bool:
        """Check whether an entity exists by id."""
        return await self.exists_by(self._effective_id_attribute, obj_id)

    async def exists_by(self, field: str, value: Any) -> bool:
        """Check whether an entity exists by an arbitrary column field."""
        async with self._session_scope() as scoped_session:
            stmt = select(exists().where(self._column_for_field(field) == value))
            result = await scoped_session.execute(stmt)
            return bool(result.scalar())


class SQLAlchemyUpdateMixin(SQLAlchemyContextMixin[OutputT, IdT], Generic[OutputT, IdT]):
    """Mixin providing the ``update`` operation for SQLAlchemy repositories."""

    @handle_integrity_errors
    async def update(self, obj_id: IdT, data: msgspec.Struct) -> OutputT | None:
        """Apply partial updates and return updated output struct."""
        async with self._session_scope() as scoped_session:
            sa_model = self._effective_sa_model
            obj = await scoped_session.get(sa_model, obj_id)
            if obj is None:
                return None

            input_data = self._serialize_input(data)
            id_attr = self._effective_id_attribute
            values = {key: value for key, value in input_data.items() if key != id_attr}

            changed_fields: set[str] = set()
            for field_name, field_value in values.items():
                if getattr(obj, field_name, _SENTINEL) != field_value:
                    setattr(obj, field_name, field_value)
                    changed_fields.add(field_name)

            if changed_fields:
                await scoped_session.flush()
                await scoped_session.refresh(obj)
                record_mutation(
                    MutationEvent(
                        entity=self.entity_name,
                        op="update",
                        ids=(obj_id,),
                        changed_fields=frozenset(changed_fields),
                        tags=self._mutation_tags(obj),
                    )
                )

            return cast(OutputT, self._to_output(obj))


class SQLAlchemyDeleteMixin(SQLAlchemyContextMixin[OutputT, IdT], Generic[OutputT, IdT]):
    """Mixin providing the ``delete`` operation for SQLAlchemy repositories."""

    @handle_integrity_errors
    async def delete(self, obj_id: IdT) -> bool:
        """Delete one entity by id."""
        async with self._session_scope() as scoped_session:
            sa_model = self._effective_sa_model
            obj = await scoped_session.get(sa_model, obj_id)
            if obj is None:
                return False
            tags = self._mutation_tags(obj)
            await scoped_session.delete(obj)

            record_mutation(
                MutationEvent(
                    entity=self.entity_name,
                    op="delete",
                    ids=(obj_id,),
                    tags=tags,
                )
            )
            return True


def _to_internal_field_names(
    struct_type: type[msgspec.Struct],
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Map external encoded names (camelCase) to internal field names (snake_case)."""
    encoded_to_internal = {
        (field.encode_name or field.name): field.name
        for field in msgspec.structs.fields(struct_type)
    }
    return {encoded_to_internal.get(key, key): value for key, value in payload.items()}
