from __future__ import annotations

from typing import Annotated

import msgspec
from pytest import raises

from loom.core.model import (
    BaseModel,
    Boolean,
    Cardinality,
    Field,
    Float,
    Integer,
    OnDelete,
    Projection,
    Relation,
    String,
    get_column_fields,
    get_id_attribute,
    get_projections,
    get_relations,
    get_table_name,
)


class _Product(BaseModel):
    __tablename__ = "products"
    id: Annotated[int, Integer, Field(primary_key=True, autoincrement=True)]
    name: Annotated[str, String(120)]
    price: Annotated[float, Float]
    active: Annotated[bool, Boolean, Field(default=True)]

    reviews: list[dict] = Relation(
        foreign_key="product_id",
        cardinality=Cardinality.ONE_TO_MANY,
        on_delete=OnDelete.CASCADE,
        profiles=("with_details",),
        depends_on=("reviews:product_id",),
    )
    count_reviews: int = Projection(
        loader=None,
        profiles=("with_details",),
        depends_on=("reviews:product_id",),
        default=0,
    )


class TestModelIntrospection:
    def test_column_fields(self) -> None:
        fields = get_column_fields(_Product)
        assert set(fields.keys()) == {"id", "name", "price", "active"}
        assert fields["id"].field.primary_key is True
        assert fields["name"].column_type.type_name == "String"
        assert fields["name"].column_type.args == (120,)

    def test_id_attribute_and_table_name(self) -> None:
        assert get_id_attribute(_Product) == "id"
        assert get_table_name(_Product) == "products"

    def test_relations_and_projections(self) -> None:
        assert "reviews" in get_relations(_Product)
        assert get_relations(_Product)["reviews"].cardinality == Cardinality.ONE_TO_MANY
        assert "count_reviews" in get_projections(_Product)
        assert get_projections(_Product)["count_reviews"].default == 0


class TestStructBehavior:
    def test_frozen_and_omit_defaults(self) -> None:
        p = _Product(id=1, name="x", price=1.0, active=True)
        data = msgspec.json.decode(msgspec.json.encode(p))
        # Column fields always present
        assert set(data.keys()) == {"id", "name", "price", "active"}
        # Frozen
        with raises(AttributeError):
            p.name = "y"  # type: ignore[misc]

    def test_camel_rename_and_loaded_fields(self) -> None:
        p = _Product(id=1, name="x", price=1.0, active=True, count_reviews=5)
        data = msgspec.json.decode(msgspec.json.encode(p))
        assert data["countReviews"] == 5
