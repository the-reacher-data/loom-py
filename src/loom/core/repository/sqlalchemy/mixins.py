from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import Any, cast

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
    FilterParams,
    IdT,
    OutputT,
    PageParams,
    PageResult,
    build_page_result,
)
from loom.core.repository.mutation import MutationEvent
from loom.core.repository.sqlalchemy.transactional import record_mutation

_SENTINEL = object()


class SQLAlchemyContextMixin:
    """Shared context and helper methods for all SQLAlchemy repository mixins."""

    model: type[Any]
    _sa_model: type[Any] | None = None
    _id_attr: str | None = None
    _column_field_names: frozenset[str] | None = None

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
        self._column_field_names = frozenset(get_column_fields(self.model).keys())

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
            return builtins
        return dict(data)

    def _get_profile_options(self, profile: str) -> list[Any]:
        from sqlalchemy.orm import selectinload

        sa_model = self._effective_sa_model
        relations = get_relations(self.model)
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
        mapper = inspect(obj).mapper
        column_names = self._column_field_names or frozenset()

        kwargs: dict[str, Any] = {}
        for col in mapper.column_attrs:
            if col.key in column_names:
                kwargs[col.key] = getattr(obj, col.key)

        relations = get_relations(self.model)
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

        mapper = inspect(obj).mapper
        for column in mapper.column_attrs:
            value = getattr(obj, column.key, None)
            if value is not None:
                tags.add(f"{table_name}:{column.key}:{value}")
        return frozenset(tags)

    def _projections_for_profile(self, profile: str) -> dict[str, Any]:
        """Return projections active for the given profile."""
        struct_projections = get_projections(self.model)
        return {name: proj for name, proj in struct_projections.items() if profile in proj.profiles}

    async def _collect_projection_values(
        self,
        scoped_session: AsyncSession,
        objs: list[Any],
        profile: str,
    ) -> dict[int, dict[str, Any]]:
        """Batch-load projection values for a list of ORM objects."""
        if not objs:
            return {}
        projections = self._projections_for_profile(profile)
        if not projections:
            return {}

        id_attr = self._effective_id_attribute
        obj_ids = [getattr(obj, id_attr) for obj in objs]
        values_by_index: dict[int, dict[str, Any]] = {i: {} for i in range(len(objs))}

        for field_name, projection in projections.items():
            loader = projection.loader
            if loader is None:
                continue
            rows = await loader.load_many(scoped_session, obj_ids)
            default = projection.default
            for i, oid in enumerate(obj_ids):
                values_by_index[i][field_name] = rows.get(oid, default)

        return values_by_index

    def _session_scope(
        self, session: AsyncSession | None = None
    ) -> AbstractAsyncContextManager[AsyncSession]:
        raise NotImplementedError


class SQLAlchemyCreateMixin(SQLAlchemyContextMixin):
    """Mixin providing the ``create`` operation for SQLAlchemy repositories."""

    async def create(self, data: msgspec.Struct) -> OutputT:
        """Persist one entity and return its output struct."""
        async with self._session_scope() as scoped_session:
            sa_model = self._effective_sa_model
            obj = sa_model(**self._serialize_input(data))
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
                    changed_fields=frozenset(self._serialize_input(data).keys()),
                    tags=self._mutation_tags(obj),
                )
            )
            return cast(OutputT, self._to_output(obj))


class SQLAlchemyReadMixin(SQLAlchemyContextMixin):
    """Mixin providing read operations for SQLAlchemy repositories."""

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

    async def exists(self, obj_id: IdT) -> bool:
        """Check whether an entity exists by id."""
        async with self._session_scope() as scoped_session:
            stmt = select(exists().where(self._id_column() == obj_id))
            result = await scoped_session.execute(stmt)
            return bool(result.scalar())


class SQLAlchemyUpdateMixin(SQLAlchemyContextMixin):
    """Mixin providing the ``update`` operation for SQLAlchemy repositories."""

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


class SQLAlchemyDeleteMixin(SQLAlchemyContextMixin):
    """Mixin providing the ``delete`` operation for SQLAlchemy repositories."""

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
