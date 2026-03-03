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
    TimestampedModel,
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


class _AuditEntity(TimestampedModel):
    __tablename__ = "test_audit_entities"
    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=80)


class _LegacyAuditEntity(BaseModel):
    __tablename__ = "test_legacy_audit_entities"
    id: int = ColumnField(primary_key=True, autoincrement=True)
    updated_at: str = ColumnField(server_onupdate="now", nullable=True)


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

    def test_timestamped_model_does_not_emit_unset_defaults(self) -> None:
        sa_cls = cast(Any, compile_model(_AuditEntity))
        created = sa_cls.__table__.c.created_at
        updated = sa_cls.__table__.c.updated_at

        assert created.default is None
        assert updated.default is None
        assert created.server_default is not None
        assert updated.server_default is not None
        assert updated.onupdate is not None

    def test_server_onupdate_string_now_is_supported_for_backward_compatibility(self) -> None:
        sa_cls = cast(Any, compile_model(_LegacyAuditEntity))
        updated = sa_cls.__table__.c.updated_at
        assert updated.onupdate is not None


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
