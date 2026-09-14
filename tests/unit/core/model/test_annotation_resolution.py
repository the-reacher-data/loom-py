import importlib
import sys
from typing import Annotated

import msgspec
import pytest
from msgspec import UnsetType

from loom.core.backend.sqlalchemy import compile_model
from loom.core.model import BaseModel, Cardinality, ColumnField, ProjectionField, RelationField
from tests.unit.core.model import _string_annotated_models as string_models


class _Shelf(BaseModel):
    __tablename__ = "annotation_resolution_shelves"
    id: int = ColumnField(primary_key=True, autoincrement=True)
    label: str = ColumnField(length=40)
    books: list[dict[str, object]] = RelationField(
        foreign_key="shelf_id",
        cardinality=Cardinality.ONE_TO_MANY,
    )
    book_count: int = ProjectionField(loader=None, default=0)
    rating: Annotated[int, "stars"] = ProjectionField(loader=None, default=0)


class _LabelledShelf(_Shelf):
    colour: str = ColumnField(length=20)


def _field_types(model: type[msgspec.Struct]) -> dict[str, object]:
    return {field.name: field.type for field in msgspec.structs.fields(model)}


def test_eager_annotations_become_struct_fields() -> None:
    assert list(_field_types(_Shelf)) == ["id", "label", "books", "book_count", "rating"]


def test_relation_and_projection_annotations_are_widened_to_unset() -> None:
    types = _field_types(_Shelf)
    assert types["books"] == list[dict[str, object]] | UnsetType
    assert types["book_count"] == int | UnsetType


def test_string_annotations_are_resolved_and_widened() -> None:
    types = _field_types(string_models.Crate)
    assert types["items"] == list[dict[str, object]] | UnsetType
    assert types["weight"] == Annotated[float, "kg"] | UnsetType


def test_annotated_projection_keeps_its_metadata() -> None:
    assert _field_types(_Shelf)["rating"] == Annotated[int, "stars"] | UnsetType


def test_subclass_keeps_inherited_fields() -> None:
    assert list(_field_types(_LabelledShelf)) == [
        "id",
        "label",
        "books",
        "book_count",
        "rating",
        "colour",
    ]


def test_compiled_table_keeps_its_primary_key() -> None:
    table = compile_model(_Shelf).__table__
    assert [column.name for column in table.primary_key.columns] == ["id"]


def test_relation_to_a_class_local_to_a_function_is_widened() -> None:
    class Page(msgspec.Struct):
        number: int

    class Chapter(BaseModel):
        __tablename__ = "annotation_resolution_chapters"
        id: int = ColumnField(primary_key=True, autoincrement=True)
        pages: list[Page] = RelationField(
            foreign_key="chapter_id",
            cardinality=Cardinality.ONE_TO_MANY,
        )

    assert _field_types(Chapter)["pages"] == list[Page] | UnsetType


@pytest.mark.skipif(sys.version_info < (3, 14), reason="needs lazy class annotations")
def test_relation_to_a_class_defined_later_resolves_once_it_exists() -> None:
    models = importlib.import_module("tests.unit.core.model._later_defined_models")
    assert _field_types(models.Reader)["loans"] == list[models.Loan] | UnsetType
    assert [column.name for column in compile_model(models.Reader).__table__.columns] == ["id"]
