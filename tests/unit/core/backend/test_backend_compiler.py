from __future__ import annotations

from collections.abc import Generator
from typing import Any, cast

from pytest import fixture

from loom.core.backend.sqlalchemy import (
    compile_all,
    compile_model,
    get_compiled,
    get_metadata,
    reset_registry,
)
from loom.core.model import (
    BaseModel,
    Cardinality,
    ColumnField,
    OnDelete,
    RelationField,
)


class _Product(BaseModel):
    __tablename__ = "test_products"
    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=120)
    price: float = ColumnField()

    reviews: list[dict[str, object]] = RelationField(
        foreign_key="product_id",
        cardinality=Cardinality.ONE_TO_MANY,
        on_delete=OnDelete.CASCADE,
        profiles=("with_details",),
        depends_on=("test_reviews:product_id",),
    )


class _Review(BaseModel):
    __tablename__ = "test_reviews"
    id: int = ColumnField(primary_key=True, autoincrement=True)
    product_id: int = ColumnField(foreign_key="test_products.id", on_delete=OnDelete.CASCADE)
    comment: str = ColumnField(length=255)


@fixture(autouse=True)
def _clean_registry() -> Generator[None, None, None]:
    reset_registry()
    yield
    reset_registry()


class TestCompileModel:
    def test_columns_and_pk(self) -> None:
        sa_cls = cast(Any, compile_model(_Product))
        table = sa_cls.__table__
        assert sa_cls.__tablename__ == "test_products"
        assert {c.name for c in table.columns} >= {"id", "name", "price"}
        assert [c.name for c in table.primary_key.columns] == ["id"]

    def test_foreign_key_on_delete(self) -> None:
        compile_model(_Product)
        sa_cls = cast(Any, compile_model(_Review))
        fk = next(iter(sa_cls.__table__.c.product_id.foreign_keys))
        assert fk.column.table.name == "test_products"
        assert fk.ondelete == "CASCADE"


class TestCompileAll:
    def test_batch_compile_and_relationships(self) -> None:
        compile_all(_Product, _Review)
        tables = set(get_metadata().tables.keys())
        assert {"test_products", "test_reviews"} <= tables

        sa_product = get_compiled(_Product)
        assert sa_product is not None
        assert hasattr(sa_product, "reviews")

    def test_idempotent(self) -> None:
        first = compile_model(_Product)
        second = compile_model(_Product)
        assert first is second

    def test_unknown_returns_none(self) -> None:
        assert get_compiled(_Product) is None
