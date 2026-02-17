from __future__ import annotations

from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager
from typing import Any, ClassVar, cast

import msgspec
from sqlalchemy import exists, func, inspect, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.core.repository.abc import (
    FilterParams,
    IdT,
    OutputT,
    PageParams,
    PageResult,
    build_page_result,
)
from loom.core.repository.mutation import MutationEvent
from loom.core.repository.sqlalchemy.projection import Projection
from loom.core.repository.sqlalchemy.transactional import record_mutation

_SENTINEL = object()


class SQLAlchemyContextMixin:
    """Shared context and helper methods for all SQLAlchemy repository mixins."""

    model: type[Any]
    id_attribute: ClassVar[str] = "id"
    output_fields: ClassVar[tuple[str, ...]] = ()
    output_struct: ClassVar[type[msgspec.Struct] | None] = None
    include_loaded_relationships: ClassVar[bool] = True
    _output_struct_cache: ClassVar[dict[tuple[str, tuple[str, ...]], type[msgspec.Struct]]] = {}

    def create_object(self, data: msgspec.Struct) -> Any:
        """Create a model instance from a struct without persisting it."""
        return self.model(**self._serialize_input(data))

    @property
    def entity_name(self) -> str:
        """Return normalized entity name for mutation tracking."""
        return self.model.__name__.lower()

    def _id_column(self) -> Any:
        return getattr(self.model, self.id_attribute)

    def _serialize_input(self, data: msgspec.Struct | Mapping[str, Any]) -> dict[str, Any]:
        if isinstance(data, msgspec.Struct):
            builtins = msgspec.to_builtins(data)
            if not isinstance(builtins, dict):
                raise TypeError("Struct payload must serialize to dict")
            return builtins
        return dict(data)

    def _get_profile_options(self, profile: str) -> list[Any]:
        getter = getattr(self.model, "get_profile_options", None)
        if callable(getter):
            return list(getter(profile))
        return []

    def _resolve_output_struct(self) -> type[msgspec.Struct]:
        cls = type(self)
        if cls.output_struct is not None:
            return cls.output_struct

        mapper = inspect(self.model)
        field_names = [column.key for column in mapper.column_attrs]
        field_names.extend(self.output_fields)
        unique_fields = []
        seen: set[str] = set()
        for name in field_names:
            if name in seen:
                continue
            seen.add(name)
            unique_fields.append((name, Any))

        struct_cls = msgspec.defstruct(
            f"{self.model.__name__}Output",
            unique_fields,
            kw_only=True,
        )
        cls.output_struct = struct_cls
        return struct_cls

    def _resolve_output_struct_for_payload(self, payload: dict[str, Any]) -> type[msgspec.Struct]:
        if self.output_struct is not None:
            return self.output_struct

        keys = tuple(sorted(payload.keys()))
        qualified_name = self.model.__qualname__
        cache_key = (qualified_name, keys)
        cached = self._output_struct_cache.get(cache_key)
        if cached is not None:
            return cached

        fields = [(key, Any) for key in keys]
        struct_cls = msgspec.defstruct(
            f"{qualified_name}Output_{abs(hash(keys))}",
            fields,
            kw_only=True,
        )
        self._output_struct_cache[cache_key] = struct_cls
        return struct_cls

    def _serialize_related(self, value: Any) -> Any:
        if value is None:
            return None

        if isinstance(value, list):
            return [self._serialize_related(item) for item in value]

        if hasattr(value, "__mapper__"):
            rel_mapper = inspect(value).mapper
            return {column.key: getattr(value, column.key) for column in rel_mapper.column_attrs}

        return value

    def _to_output(self, obj: Any) -> msgspec.Struct:
        mapper = inspect(obj).mapper
        payload = {column.key: getattr(obj, column.key) for column in mapper.column_attrs}

        for field_name in self.output_fields:
            payload[field_name] = getattr(obj, field_name, None)

        for projection_name, projection in self._projection_map().items():
            if projection.has_value(obj):
                payload[projection_name] = getattr(obj, projection_name)

        if self.include_loaded_relationships:
            for relationship in mapper.relationships:
                rel_name = relationship.key
                if rel_name in obj.__dict__:
                    payload[rel_name] = self._serialize_related(getattr(obj, rel_name))

        struct_cls = self._resolve_output_struct_for_payload(payload)
        return struct_cls(**payload)

    def to_output_from_payload(self, payload: Mapping[str, Any]) -> msgspec.Struct:
        """Build dynamic msgspec output from cached dict payload."""
        payload_dict = dict(payload)
        struct_cls = self._resolve_output_struct_for_payload(payload_dict)
        return struct_cls(**payload_dict)

    def _projection_map(self) -> dict[str, Projection[Any]]:
        result: dict[str, Projection[Any]] = {}
        for name, value in vars(self.model).items():
            if isinstance(value, Projection):
                result[name] = value
        return result

    def _mutation_tags(self, obj: Any) -> frozenset[str]:
        mapper = inspect(obj).mapper
        table_name = str(getattr(self.model, "__tablename__", self.entity_name))
        obj_id = getattr(obj, self.id_attribute, None)

        tags: set[str] = {table_name, f"{table_name}:list"}
        if obj_id is not None:
            tags.add(f"{table_name}:id:{obj_id}")

        for column in mapper.column_attrs:
            value = getattr(obj, column.key, None)
            if value is not None:
                tags.add(f"{table_name}:{column.key}:{value}")
        return frozenset(tags)

    def _projections_for_profile(self, profile: str) -> dict[str, Projection[Any]]:
        result: dict[str, Projection[Any]] = {}
        for name, projection in self._projection_map().items():
            if profile in projection.profiles:
                result[name] = projection
        return result

    async def _populate_projection_values(
        self,
        scoped_session: AsyncSession,
        objs: list[Any],
        profile: str,
    ) -> None:
        if not objs:
            return
        projections = self._projections_for_profile(profile)
        if not projections:
            return

        obj_ids = [getattr(obj, self.id_attribute) for obj in objs]
        for field_name, projection in projections.items():
            loader = projection.loader
            if loader is None:
                continue
            rows = await loader.load_many(scoped_session, obj_ids)
            for obj in objs:
                obj_id = getattr(obj, self.id_attribute)
                if obj_id in rows:
                    setattr(obj, field_name, rows[obj_id])
                else:
                    setattr(obj, field_name, projection.default)

    def _session_scope(
        self, session: AsyncSession | None = None
    ) -> AbstractAsyncContextManager[AsyncSession]:
        raise NotImplementedError


class SQLAlchemyCreateMixin(SQLAlchemyContextMixin):
    """Mixin providing the ``create`` operation for SQLAlchemy repositories."""

    async def create(self, data: msgspec.Struct) -> OutputT:
        """Persist one entity and return its output struct."""
        async with self._session_scope() as scoped_session:
            obj = self.model(**self._serialize_input(data))
            scoped_session.add(obj)
            await scoped_session.flush()
            await scoped_session.refresh(obj)

            obj_id = getattr(obj, self.id_attribute, None)
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
    """Mixin providing read operations (``get_by_id``, ``list_paginated``, ``exists``) for SQLAlchemy repositories."""

    async def get_by_id(
        self,
        obj_id: IdT,
        profile: str = "default",
    ) -> OutputT | None:
        """Fetch one entity by id with optional profile-aware loading."""
        async with self._session_scope() as scoped_session:
            options = self._get_profile_options(profile)

            if options:
                stmt = (
                    select(self.model)
                    .where(self._id_column() == obj_id)
                    .options(*options)
                    .execution_options(populate_existing=True)
                )
                result = await scoped_session.execute(stmt)
                obj = result.scalar_one_or_none()
            else:
                obj = await scoped_session.get(self.model, obj_id)

            if obj is None:
                return None
            await self._populate_projection_values(scoped_session, [obj], profile)
            return cast(OutputT, self._to_output(obj))

    async def list_paginated(
        self,
        page_params: PageParams,
        filter_params: FilterParams | None = None,
        profile: str = "default",
    ) -> PageResult[OutputT]:
        """Fetch a paginated entity list and total count."""
        async with self._session_scope() as scoped_session:
            options = self._get_profile_options(profile)
            stmt = select(self.model).order_by(self._id_column())
            if options:
                stmt = stmt.options(*options)

            count_stmt = select(func.count()).select_from(self.model)

            if filter_params and filter_params.filters:
                stmt = stmt.filter_by(**filter_params.filters)
                count_stmt = count_stmt.filter_by(**filter_params.filters)

            items_result = await scoped_session.execute(
                stmt.offset(page_params.offset).limit(page_params.limit)
            )
            total_result = await scoped_session.execute(count_stmt)

            loaded_items = list(items_result.scalars().all())
            await self._populate_projection_values(scoped_session, loaded_items, profile)
            items: list[OutputT] = [cast(OutputT, self._to_output(item)) for item in loaded_items]
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
            obj = await scoped_session.get(self.model, obj_id)
            if obj is None:
                return None

            input_data = self._serialize_input(data)
            values = {key: value for key, value in input_data.items() if key != self.id_attribute}

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
            obj = await scoped_session.get(self.model, obj_id)
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
