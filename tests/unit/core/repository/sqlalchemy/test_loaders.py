"""Unit tests for relation-based projection loaders."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from loom.core.backend.sqlalchemy import compile_all, reset_registry
from loom.core.model import BaseModel, Cardinality
from loom.core.model.field import ColumnField
from loom.core.model.projection import Projection, ProjectionField, ProjectionSource
from loom.core.model.relation import Relation
from loom.core.projection.loaders import (
    RelationCountLoader,
    RelationExistsLoader,
    RelationJoinFieldsLoader,
)
from loom.core.repository.sqlalchemy.mixins import SQLAlchemyContextMixin

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeNote:
    def __init__(self, note_id: int, title: str) -> None:
        self.id = note_id
        self.title = title


def _obj_with_relation(relation_name: str, related: list[Any]) -> Any:
    obj = MagicMock()
    obj.__dict__[relation_name] = related
    setattr(obj, relation_name, related)
    return obj


# ---------------------------------------------------------------------------
# Compilable models for integration-style mixin tests
# ---------------------------------------------------------------------------


class _NoteModel(BaseModel):
    __tablename__ = "test_notes_crl"
    id: int = ColumnField(primary_key=True, autoincrement=True)
    record_id: int = ColumnField(foreign_key="test_records_crl.id")
    title: str = ColumnField(length=120)


class _RecordWithValidConfig(BaseModel):
    __tablename__ = "test_records_crl"
    id: int = ColumnField(primary_key=True, autoincrement=True)

    notes: Any = Relation(
        foreign_key="record_id",
        cardinality=Cardinality.ONE_TO_MANY,
        profiles=("with_details",),
    )
    notes_count: int = ProjectionField(
        loader=RelationCountLoader(relation="notes"),
        source=ProjectionSource.PRELOADED,
        profiles=("with_details",),
        default=0,
    )


# ---------------------------------------------------------------------------
# load_from_object — happy path
# ---------------------------------------------------------------------------


class TestRelationLoadersLoadFromObject:
    def test_fn_applied_to_related_list(self) -> None:
        notes = [_FakeNote(1, "a"), _FakeNote(2, "b")]
        loader = RelationCountLoader(relation="notes")
        obj = _obj_with_relation("notes", notes)

        assert loader.load_from_object(obj) == 2

    def test_fn_receives_full_list(self) -> None:
        notes = [_FakeNote(1, "x"), _FakeNote(2, "y"), _FakeNote(3, "z")]
        loader = RelationJoinFieldsLoader(
            relation="notes",
            value_columns=("title",),
        )
        obj = _obj_with_relation("notes", notes)

        assert loader.load_from_object(obj) == [
            {"title": "x"},
            {"title": "y"},
            {"title": "z"},
        ]

    def test_empty_relation_passes_empty_list_to_fn(self) -> None:
        loader = RelationExistsLoader(relation="notes")
        obj = _obj_with_relation("notes", [])

        assert loader.load_from_object(obj) is False

    def test_none_relation_treated_as_empty_list(self) -> None:
        loader = RelationCountLoader(relation="notes")
        obj = MagicMock()
        obj.__dict__["notes"] = None
        obj.notes = None

        assert loader.load_from_object(obj) == 0

    def test_single_object_relation_wrapped_in_list(self) -> None:
        owner = _FakeNote(7, "owner")
        loader = RelationJoinFieldsLoader(
            relation="owner",
            value_columns=("id",),
        )
        obj = MagicMock()
        obj.__dict__["owner"] = owner
        obj.owner = owner

        assert loader.load_from_object(obj) == [{"id": 7}]


# ---------------------------------------------------------------------------
# load_from_object — safety check (Option C)
# ---------------------------------------------------------------------------


class TestRelationLoadersSafetyCheck:
    def test_raises_when_relation_not_in_dict(self) -> None:
        loader = RelationCountLoader(relation="notes")

        class _Bare:
            pass

        with pytest.raises(RuntimeError, match="notes.*__dict__"):
            loader.load_from_object(_Bare())

    def test_error_includes_model_class_name(self) -> None:
        loader = RelationCountLoader(relation="items")

        class MyEntity:
            pass

        with pytest.raises(RuntimeError, match="MyEntity"):
            loader.load_from_object(MyEntity())

    def test_error_includes_relation_name(self) -> None:
        loader = RelationCountLoader(relation="attachments")

        class SomeModel:
            pass

        with pytest.raises(RuntimeError, match="attachments"):
            loader.load_from_object(SomeModel())


# ---------------------------------------------------------------------------
# _validate_computed_relation_loaders (Option A)
#
# We call _validate_computed_relation_loaders() directly, injecting
# _projections_cache and _relations_cache without requiring SA compilation,
# to avoid FK-setup noise in unit tests.
# ---------------------------------------------------------------------------


def _make_validation_mixin(
    model_name: str,
    projections: dict[str, Projection],
    relations: dict[str, Relation],
) -> SQLAlchemyContextMixin[Any, Any]:
    mixin: SQLAlchemyContextMixin[Any, Any] = object.__new__(SQLAlchemyContextMixin)
    mixin.model = type(model_name, (), {})
    mixin.model.__name__ = model_name
    mixin._projections_cache = projections
    mixin._relations_cache = relations
    return mixin


class TestValidateComputedRelationLoaders:
    def test_valid_config_does_not_raise(self) -> None:
        relations = {
            "notes": Relation(
                foreign_key="record_id",
                cardinality=Cardinality.ONE_TO_MANY,
                profiles=("with_details",),
            )
        }
        projections = {
            "notes_count": Projection(
                loader=RelationCountLoader(relation="notes"),
                source=ProjectionSource.PRELOADED,
                profiles=("with_details",),
                default=0,
            )
        }
        mixin = _make_validation_mixin("MyModel", projections, relations)
        mixin._validate_computed_relation_loaders()  # must not raise

    def test_missing_relation_raises_value_error(self) -> None:
        projections = {
            "bad_count": Projection(
                loader=RelationCountLoader(relation="ghost"),
                source=ProjectionSource.PRELOADED,
                profiles=("with_details",),
                default=0,
            )
        }
        mixin = _make_validation_mixin("MyModel", projections, {})

        with pytest.raises(ValueError, match="ghost.*does not exist"):
            mixin._validate_computed_relation_loaders()

    def test_profile_mismatch_raises_value_error(self) -> None:
        relations = {
            "notes": Relation(
                foreign_key="record_id",
                cardinality=Cardinality.ONE_TO_MANY,
                profiles=("with_details",),
            )
        }
        projections = {
            "has_notes": Projection(
                loader=RelationExistsLoader(relation="notes"),
                source=ProjectionSource.PRELOADED,
                profiles=("with_details", "summary"),
                default=False,
            )
        }
        mixin = _make_validation_mixin("MyModel", projections, relations)

        with pytest.raises(ValueError, match="summary.*notes.*not loaded"):
            mixin._validate_computed_relation_loaders()

    def test_error_includes_projection_field_name(self) -> None:
        projections = {
            "bad_count": Projection(
                loader=RelationCountLoader(relation="ghost"),
                source=ProjectionSource.PRELOADED,
                profiles=("p",),
                default=0,
            )
        }
        mixin = _make_validation_mixin("MyModel", projections, {})

        with pytest.raises(ValueError, match="bad_count"):
            mixin._validate_computed_relation_loaders()

    def test_error_includes_model_name(self) -> None:
        relations = {
            "notes": Relation(
                foreign_key="record_id",
                cardinality=Cardinality.ONE_TO_MANY,
                profiles=("with_details",),
            )
        }
        projections = {
            "has_notes": Projection(
                loader=RelationExistsLoader(relation="notes"),
                source=ProjectionSource.PRELOADED,
                profiles=("with_details", "summary"),
                default=False,
            )
        }
        mixin = _make_validation_mixin("BenchRecord", projections, relations)

        with pytest.raises(ValueError, match="BenchRecord"):
            mixin._validate_computed_relation_loaders()

    def test_non_computed_projections_are_ignored(self) -> None:
        """Regular DB-backed projections must not trigger relation validation."""
        projections = {
            "notes_count": Projection(
                loader=MagicMock(),  # any non-relation memory loader
                profiles=("with_details",),
                default=0,
            )
        }
        mixin = _make_validation_mixin("MyModel", projections, {})
        mixin._validate_computed_relation_loaders()  # must not raise


# ---------------------------------------------------------------------------
# _collect_projection_values — memory vs DB path + gather
# ---------------------------------------------------------------------------


class TestCollectProjectionValues:
    @pytest.fixture(autouse=True)
    def _compile(self) -> Any:
        reset_registry()
        compile_all(_NoteModel, _RecordWithValidConfig)
        yield
        reset_registry()

    def _make_mixin(self) -> SQLAlchemyContextMixin[Any, Any]:
        mixin: SQLAlchemyContextMixin[Any, Any] = object.__new__(SQLAlchemyContextMixin)
        mixin.model = _RecordWithValidConfig
        mixin._sa_model = None
        mixin._id_attr = None
        mixin._column_field_names = None
        mixin._output_column_keys = None
        mixin._all_sa_column_keys = None
        mixin._relations_cache = None
        mixin._projections_cache = None
        mixin._init_struct_model()
        return mixin

    async def test_memory_projection_does_not_call_session(self) -> None:
        mixin = self._make_mixin()
        session = AsyncMock()

        notes = [_FakeNote(1, "a"), _FakeNote(2, "b")]
        obj = MagicMock()
        obj.id = 10
        obj.__dict__["notes"] = notes
        obj.notes = notes

        result = await mixin._collect_projection_values(session, [obj], "with_details")

        assert result[0]["notes_count"] == 2
        session.execute.assert_not_called()

    async def test_db_projections_use_gather(self) -> None:
        """Multiple DB-backed loaders must be fired via asyncio.gather."""

        class _MultiDB(BaseModel):
            __tablename__ = "test_multi_db_crl"
            id: int = ColumnField(primary_key=True, autoincrement=True)

            cnt: int = ProjectionField(
                loader=MagicMock(load_many=AsyncMock(return_value={1: 3})),
                profiles=("detail",),
                default=0,
            )
            flag: bool = ProjectionField(
                loader=MagicMock(load_many=AsyncMock(return_value={1: True})),
                profiles=("detail",),
                default=False,
            )

        reset_registry()
        compile_all(_MultiDB)

        mixin: SQLAlchemyContextMixin[Any, Any] = object.__new__(SQLAlchemyContextMixin)
        mixin.model = _MultiDB
        mixin._sa_model = None
        mixin._id_attr = None
        mixin._column_field_names = None
        mixin._output_column_keys = None
        mixin._all_sa_column_keys = None
        mixin._relations_cache = None
        mixin._projections_cache = None
        mixin._init_struct_model()

        session = AsyncMock()
        obj = MagicMock()
        obj.id = 1

        with patch(
            "loom.core.projection.runtime.asyncio.gather",
            wraps=asyncio.gather,
        ) as mock_gather:
            result = await mixin._collect_projection_values(session, [obj], "detail")

        mock_gather.assert_called_once()
        assert result[0]["cnt"] == 3
        assert result[0]["flag"] is True

    async def test_empty_objs_returns_empty(self) -> None:
        mixin = self._make_mixin()
        result = await mixin._collect_projection_values(AsyncMock(), [], "with_details")
        assert result == {}

    async def test_profile_with_no_projections_returns_empty(self) -> None:
        mixin = self._make_mixin()
        obj = MagicMock()
        obj.id = 1
        result = await mixin._collect_projection_values(AsyncMock(), [obj], "default")
        assert result == {}

    async def test_default_applied_when_db_loader_returns_no_entry(self) -> None:
        class _WithDefault(BaseModel):
            __tablename__ = "test_with_default_crl"
            id: int = ColumnField(primary_key=True, autoincrement=True)

            score: int = ProjectionField(
                loader=MagicMock(load_many=AsyncMock(return_value={})),
                profiles=("p",),
                default=-1,
            )

        reset_registry()
        compile_all(_WithDefault)

        mixin: SQLAlchemyContextMixin[Any, Any] = object.__new__(SQLAlchemyContextMixin)
        mixin.model = _WithDefault
        mixin._sa_model = None
        mixin._id_attr = None
        mixin._column_field_names = None
        mixin._output_column_keys = None
        mixin._all_sa_column_keys = None
        mixin._relations_cache = None
        mixin._projections_cache = None
        mixin._init_struct_model()

        obj = MagicMock()
        obj.id = 99
        result = await mixin._collect_projection_values(AsyncMock(), [obj], "p")
        assert result[0]["score"] == -1

    async def test_projection_without_default_is_omitted_when_loader_returns_no_entry(self) -> None:
        class _WithoutDefault(BaseModel):
            __tablename__ = "test_without_default_crl"
            id: int = ColumnField(primary_key=True, autoincrement=True)

            score: int = ProjectionField(
                loader=MagicMock(load_many=AsyncMock(return_value={})),
                profiles=("p",),
            )

        reset_registry()
        compile_all(_WithoutDefault)

        mixin: SQLAlchemyContextMixin[Any, Any] = object.__new__(SQLAlchemyContextMixin)
        mixin.model = _WithoutDefault
        mixin._sa_model = None
        mixin._id_attr = None
        mixin._column_field_names = None
        mixin._output_column_keys = None
        mixin._all_sa_column_keys = None
        mixin._relations_cache = None
        mixin._projections_cache = None
        mixin._init_struct_model()

        obj = MagicMock()
        obj.id = 101
        result = await mixin._collect_projection_values(AsyncMock(), [obj], "p")
        assert "score" not in result[0]

    async def test_projection_with_explicit_none_default_is_included(self) -> None:
        class _WithNoneDefault(BaseModel):
            __tablename__ = "test_with_none_default_crl"
            id: int = ColumnField(primary_key=True, autoincrement=True)

            score: int | None = ProjectionField(
                loader=MagicMock(load_many=AsyncMock(return_value={})),
                profiles=("p",),
                default=None,
            )

        reset_registry()
        compile_all(_WithNoneDefault)

        mixin: SQLAlchemyContextMixin[Any, Any] = object.__new__(SQLAlchemyContextMixin)
        mixin.model = _WithNoneDefault
        mixin._sa_model = None
        mixin._id_attr = None
        mixin._column_field_names = None
        mixin._output_column_keys = None
        mixin._all_sa_column_keys = None
        mixin._relations_cache = None
        mixin._projections_cache = None
        mixin._init_struct_model()

        obj = MagicMock()
        obj.id = 102
        result = await mixin._collect_projection_values(AsyncMock(), [obj], "p")
        assert "score" in result[0]
        assert result[0]["score"] is None
