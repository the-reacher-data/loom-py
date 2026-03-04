from __future__ import annotations

import asyncio
from contextlib import AbstractAsyncContextManager
from typing import Any, Generic, cast

import msgspec
from sqlalchemy import exists, func, inspect, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.core.model.introspection import (
    get_column_fields,
    get_id_attribute,
    get_projections,
    get_relations,
    get_table_name,
)
from loom.core.repository.abc import (
    CursorResult,
    FilterParams,
    IdT,
    OutputT,
    PageParams,
    PageResult,
    QuerySpec,
    build_page_result,
)
from loom.core.repository.mutation import MutationEvent
from loom.core.repository.sqlalchemy.integrity import handle_integrity_errors
from loom.core.repository.sqlalchemy.query_compiler.compiler import QuerySpecCompiler
from loom.core.repository.sqlalchemy.transactional import record_mutation

_SENTINEL = object()


class SQLAlchemyContextMixin(Generic[OutputT, IdT]):
    """Shared context and helper methods for all SQLAlchemy repository mixins."""

    model: type[Any]
    _sa_model: type[Any] | None = None
    _id_attr: str | None = None
    _column_field_names: frozenset[str] | None = None
    # Pre-computed at init to avoid per-request/per-object reflection.
    _output_column_keys: tuple[str, ...] | None = None
    _all_sa_column_keys: tuple[str, ...] | None = None
    _relations_cache: dict[str, Any] | None = None
    _projections_cache: dict[str, Any] | None = None

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

        sa_mapper: Any = inspect(sa).mapper
        self._output_column_keys = tuple(
            col.key for col in sa_mapper.column_attrs if col.key in column_field_names
        )
        self._all_sa_column_keys = tuple(col.key for col in sa_mapper.column_attrs)
        self._validate_computed_relation_loaders()

    def _validate_computed_relation_loaders(self) -> None:
        """Validate ComputedFromRelationLoader references at repository init time.

        Ensures every ``ProjectionField`` backed by a
        :class:`~loom.core.repository.sqlalchemy.loaders.ComputedFromRelationLoader`
        references an existing relation that is eagerly loaded in all profiles
        where the projection is active.

        Raises:
            ValueError: If the referenced relation does not exist on the model,
                or if the projection is active in profiles where the relation
                is not loaded.
        """
        from loom.core.repository.sqlalchemy.loaders import ComputedFromRelationLoader

        projections = self._projections_cache or {}
        relations = self._relations_cache or {}
        model_name = self.model.__name__

        for proj_name, proj in projections.items():
            loader = proj.loader
            if not isinstance(loader, ComputedFromRelationLoader):
                continue

            rel_name = loader.relation
            if rel_name not in relations:
                raise ValueError(
                    f"'{model_name}.{proj_name}': ComputedFromRelationLoader references "
                    f"relation '{rel_name}' which does not exist on this model."
                )

            unsupported = frozenset(proj.profiles) - frozenset(relations[rel_name].profiles)
            if unsupported:
                raise ValueError(
                    f"'{model_name}.{proj_name}': "
                    f"ComputedFromRelationLoader(relation='{rel_name}') "
                    f"is active in profiles {sorted(unsupported)} where '{rel_name}' is not "
                    f"loaded. Add {sorted(unsupported)} to '{rel_name}'.profiles or restrict "
                    f"'{proj_name}'.profiles to {sorted(relations[rel_name].profiles)}."
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

    def _get_profile_options(self, profile: str) -> list[Any]:
        from sqlalchemy.orm import selectinload

        sa_model = self._effective_sa_model
        relations = (
            self._relations_cache
            if self._relations_cache is not None
            else get_relations(self.model)
        )
        options: list[Any] = []
        for rel_name, rel in relations.items():
            if profile in rel.profiles:
                attr = getattr(sa_model, rel_name, None)
                if attr is not None:
                    options.append(selectinload(attr))
        return options

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
        """Batch-load projection values for a list of ORM objects.

        :class:`~loom.core.repository.sqlalchemy.loaders.ComputedFromRelationLoader`
        projections are resolved synchronously from already-loaded relation
        data on each ORM object (no DB round-trip).  All remaining DB-backed
        loaders are fired concurrently via :func:`asyncio.gather`.
        """
        if not objs:
            return {}
        projections = self._projections_for_profile(profile)
        if not projections:
            return {}

        from loom.core.repository.sqlalchemy.loaders import ComputedFromRelationLoader

        id_attr = self._effective_id_attribute
        obj_ids = [getattr(obj, id_attr) for obj in objs]
        values_by_index: dict[int, dict[str, Any]] = {i: {} for i in range(len(objs))}

        memory_projections = {
            name: proj
            for name, proj in projections.items()
            if isinstance(proj.loader, ComputedFromRelationLoader)
        }
        db_projections = {
            name: proj
            for name, proj in projections.items()
            if proj.loader is not None and not isinstance(proj.loader, ComputedFromRelationLoader)
        }

        for field_name, projection in memory_projections.items():
            for i, obj in enumerate(objs):
                values_by_index[i][field_name] = projection.loader.load_from_object(obj)

        if db_projections:
            results = await asyncio.gather(
                *(
                    proj.loader.load_many(scoped_session, obj_ids)
                    for proj in db_projections.values()
                )
            )
            for (field_name, proj), rows in zip(
                db_projections.items(),
                results,
                strict=False,
            ):
                default = proj.default
                for i, oid in enumerate(obj_ids):
                    values_by_index[i][field_name] = rows.get(oid, default)

        return values_by_index

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
    """Mixin providing read operations for SQLAlchemy repositories.

    Class attributes:
        allowed_filter_fields: Whitelist of field names permitted in
            :meth:`list_with_query` filters.  An empty frozenset (the default)
            allows all fields.
    """

    allowed_filter_fields: frozenset[str] = frozenset()

    async def get_by_id(
        self,
        obj_id: IdT,
        profile: str = "default",
    ) -> OutputT | None:
        """Fetch one entity by id with optional profile-aware loading."""
        async with self._session_scope() as scoped_session:
            sa_model = self._effective_sa_model
            options = self._get_profile_options(profile)

            if options:
                stmt = (
                    select(sa_model)
                    .where(self._id_column() == obj_id)
                    .options(*options)
                    .execution_options(populate_existing=True)
                )
                result = await scoped_session.execute(stmt)
                obj = result.scalar_one_or_none()
            else:
                obj = await scoped_session.get(sa_model, obj_id)

            if obj is None:
                return None

            pv = await self._collect_projection_values(
                scoped_session,
                [obj],
                profile,
            )
            return cast(
                OutputT,
                self._to_output(obj, profile=profile, projection_values=pv.get(0)),
            )

    async def get_by(
        self,
        field: str,
        value: Any,
        profile: str = "default",
    ) -> OutputT | None:
        """Fetch one entity by equality over an arbitrary column field."""
        async with self._session_scope() as scoped_session:
            sa_model = self._effective_sa_model
            if not hasattr(sa_model, field):
                raise ValueError(f"Unknown field '{field}' for model '{self.model.__name__}'")

            options = self._get_profile_options(profile)
            stmt = select(sa_model).where(getattr(sa_model, field) == value).limit(1)
            if options:
                stmt = stmt.options(*options).execution_options(populate_existing=True)

            result = await scoped_session.execute(stmt)
            obj = result.scalar_one_or_none()
            if obj is None:
                return None

            pv = await self._collect_projection_values(
                scoped_session,
                [obj],
                profile,
            )
            return cast(
                OutputT,
                self._to_output(obj, profile=profile, projection_values=pv.get(0)),
            )

    async def list_paginated(
        self,
        page_params: PageParams,
        filter_params: FilterParams | None = None,
        profile: str = "default",
    ) -> PageResult[OutputT]:
        """Fetch a paginated entity list and total count."""
        async with self._session_scope() as scoped_session:
            sa_model = self._effective_sa_model
            options = self._get_profile_options(profile)
            stmt = select(sa_model).order_by(self._id_column())
            if options:
                stmt = stmt.options(*options)

            count_stmt = select(func.count()).select_from(sa_model)

            if filter_params and filter_params.filters:
                stmt = stmt.filter_by(**filter_params.filters)
                count_stmt = count_stmt.filter_by(**filter_params.filters)

            items_result = await scoped_session.execute(
                stmt.offset(page_params.offset).limit(page_params.limit)
            )
            total_result = await scoped_session.execute(count_stmt)

            loaded_items = list(items_result.scalars().all())

            proj_map = await self._collect_projection_values(
                scoped_session,
                loaded_items,
                profile,
            )
            items: list[OutputT] = [
                cast(
                    OutputT,
                    self._to_output(
                        item,
                        profile=profile,
                        projection_values=proj_map.get(i),
                    ),
                )
                for i, item in enumerate(loaded_items)
            ]

            total_count = int(total_result.scalar() or 0)
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
        from loom.core.repository.abc.query import PaginationMode

        async with self._session_scope() as scoped_session:
            sa_model = self._effective_sa_model
            id_col = self._id_column()
            options = self._get_profile_options(profile)

            compiler = QuerySpecCompiler(sa_model, id_col, self.allowed_filter_fields)
            raw_items, total_count, next_cursor, has_next = await compiler.execute(
                scoped_session, query, options
            )

            proj_map = await self._collect_projection_values(scoped_session, raw_items, profile)
            items: list[OutputT] = [
                cast(
                    OutputT,
                    self._to_output(
                        item,
                        profile=profile,
                        projection_values=proj_map.get(i),
                    ),
                )
                for i, item in enumerate(raw_items)
            ]

            if query.pagination == PaginationMode.CURSOR:
                return CursorResult(
                    items=tuple(items),
                    next_cursor=next_cursor,
                    has_next=has_next,
                )

            from loom.core.repository.abc.query import PageParams

            page_params = PageParams(page=query.page, limit=query.limit)
            return build_page_result(items, total_count or 0, page_params)

    async def exists(self, obj_id: IdT) -> bool:
        """Check whether an entity exists by id."""
        return await self.exists_by(self._effective_id_attribute, obj_id)

    async def exists_by(self, field: str, value: Any) -> bool:
        """Check whether an entity exists by an arbitrary column field."""
        async with self._session_scope() as scoped_session:
            sa_model = self._effective_sa_model
            if not hasattr(sa_model, field):
                raise ValueError(f"Unknown field '{field}' for model '{self.model.__name__}'")

            stmt = select(exists().where(getattr(sa_model, field) == value))
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
