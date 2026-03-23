"""Tests for LoomDtype, ColumnSchema, SchemaMode, and StubCatalog schema API."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from loom.etl._schema import ColumnSchema, LoomDtype
from loom.etl._table import TableRef
from loom.etl._target import IntoTable, SchemaMode, WriteMode
from loom.etl.testing import StubCatalog


def test_loom_dtype_is_str_enum() -> None:
    assert LoomDtype.INT64 == "Int64"
    assert LoomDtype.UTF8 == "Utf8"
    assert LoomDtype.DATE == "Date"
    assert LoomDtype.BOOLEAN == "Boolean"


def test_loom_dtype_all_numeric_variants_present() -> None:
    for name in ("INT8", "INT16", "INT32", "INT64", "UINT8", "UINT16", "UINT32", "UINT64"):
        assert hasattr(LoomDtype, name)


def test_loom_dtype_float_variants() -> None:
    assert LoomDtype.FLOAT32 == "Float32"
    assert LoomDtype.FLOAT64 == "Float64"
    assert LoomDtype.DECIMAL == "Decimal"


def test_loom_dtype_temporal_variants() -> None:
    for name in ("DATE", "DATETIME", "DURATION", "TIME"):
        assert hasattr(LoomDtype, name)


def test_column_schema_stores_fields() -> None:
    col = ColumnSchema("order_id", LoomDtype.INT64, nullable=False)
    assert col.name == "order_id"
    assert col.dtype is LoomDtype.INT64
    assert col.nullable is False


def test_column_schema_nullable_default_true() -> None:
    col = ColumnSchema("amount", LoomDtype.FLOAT64)
    assert col.nullable is True


def test_column_schema_is_frozen() -> None:
    col = ColumnSchema("x", LoomDtype.UTF8)
    with pytest.raises((AttributeError, TypeError)):
        col.name = "y"  # type: ignore[misc]


def test_column_schema_equality() -> None:
    a = ColumnSchema("x", LoomDtype.INT32)
    b = ColumnSchema("x", LoomDtype.INT32)
    assert a == b


def test_column_schema_inequality_different_dtype() -> None:
    a = ColumnSchema("x", LoomDtype.INT32)
    b = ColumnSchema("x", LoomDtype.INT64)
    assert a != b


def test_column_schema_hashable() -> None:
    col1 = ColumnSchema("x", LoomDtype.UTF8)
    col2 = ColumnSchema("x", LoomDtype.UTF8)
    assert hash(col1) == hash(col2)  # equal objects produce equal hashes
    assert {col1, col2} == {col1}  # set deduplication works


def test_schema_mode_values() -> None:
    assert SchemaMode.STRICT == "strict"
    assert SchemaMode.EVOLVE == "evolve"
    assert SchemaMode.OVERWRITE == "overwrite"


@pytest.mark.parametrize(
    "build,expected_mode,expected_schema",
    [
        (lambda t: t.replace(), WriteMode.REPLACE, SchemaMode.STRICT),
        (lambda t: t.replace(schema=SchemaMode.EVOLVE), WriteMode.REPLACE, SchemaMode.EVOLVE),
        (lambda t: t.replace(schema=SchemaMode.OVERWRITE), WriteMode.REPLACE, SchemaMode.OVERWRITE),
        (lambda t: t.append(schema=SchemaMode.EVOLVE), WriteMode.APPEND, SchemaMode.EVOLVE),
        (
            lambda t: t.replace_partitions("year", schema=SchemaMode.EVOLVE),
            WriteMode.REPLACE_PARTITIONS,
            SchemaMode.EVOLVE,
        ),
        (
            lambda t: t.upsert(keys=("order_id",), schema=SchemaMode.EVOLVE),
            WriteMode.UPSERT,
            SchemaMode.EVOLVE,
        ),
    ],
)
def test_into_table_write_and_schema_mode(
    build: Callable[[IntoTable], IntoTable], expected_mode: WriteMode, expected_schema: SchemaMode
) -> None:
    spec = build(IntoTable("staging.orders"))._to_spec()
    assert spec.mode is expected_mode
    assert spec.schema_mode is expected_schema


def test_into_table_returns_new_instance_on_each_call() -> None:
    base = IntoTable("staging.orders")
    a = base.replace()
    b = base.replace(schema=SchemaMode.EVOLVE)
    assert a is not b
    assert a._to_spec().schema_mode is SchemaMode.STRICT
    assert b._to_spec().schema_mode is SchemaMode.EVOLVE


_ORDERS_SCHEMA = (
    ColumnSchema("order_id", LoomDtype.INT64, nullable=False),
    ColumnSchema("amount", LoomDtype.FLOAT64),
    ColumnSchema("year", LoomDtype.INT32, nullable=False),
)


@pytest.mark.parametrize(
    "ref,expected",
    [
        ("raw.orders", _ORDERS_SCHEMA),
        ("raw.missing", None),
    ],
)
def test_stub_catalog_schema(ref: str, expected: object) -> None:
    catalog = StubCatalog(schemas={"raw.orders": _ORDERS_SCHEMA})
    assert catalog.schema(TableRef(ref)) == expected


def test_stub_catalog_update_schema_persists() -> None:
    catalog = StubCatalog(schemas={"staging.out": ()})
    new_schema = (ColumnSchema("id", LoomDtype.INT64),)
    catalog.update_schema(TableRef("staging.out"), new_schema)
    assert catalog.schema(TableRef("staging.out")) == new_schema


def test_stub_catalog_update_schema_registers_new_table() -> None:
    catalog = StubCatalog()
    schema = (ColumnSchema("id", LoomDtype.INT64),)
    catalog.update_schema(TableRef("new.table"), schema)
    assert catalog.exists(TableRef("new.table"))
    assert catalog.schema(TableRef("new.table")) == schema


def test_stub_catalog_columns_derived_from_schemas() -> None:
    catalog = StubCatalog(schemas={"raw.orders": _ORDERS_SCHEMA})
    assert catalog.columns(TableRef("raw.orders")) == ("order_id", "amount", "year")


def test_stub_catalog_exists_derived_from_schemas() -> None:
    catalog = StubCatalog(schemas={"raw.orders": _ORDERS_SCHEMA})
    assert catalog.exists(TableRef("raw.orders"))
    assert not catalog.exists(TableRef("raw.missing"))


def test_stub_catalog_tables_and_schemas_merged() -> None:
    """Tables passed via 'tables=' are accessible via schema() with NULL dtype."""
    catalog = StubCatalog(tables={"raw.orders": ("id", "amount")})
    schema = catalog.schema(TableRef("raw.orders"))
    assert schema is not None
    assert len(schema) == 2
    assert schema[0].name == "id"
    assert schema[1].name == "amount"


def test_stub_catalog_schemas_takes_priority_over_tables() -> None:
    """If same key in both, schemas= wins."""
    catalog = StubCatalog(
        tables={"raw.orders": ("id",)},
        schemas={"raw.orders": _ORDERS_SCHEMA},
    )
    assert catalog.schema(TableRef("raw.orders")) == _ORDERS_SCHEMA


def test_stub_catalog_update_schema_visible_to_subsequent_schema_call() -> None:
    """Simulates a step writing and the next step reading the updated schema."""
    catalog = StubCatalog(schemas={"staging.out": ()})
    evolved = _ORDERS_SCHEMA + (ColumnSchema("region", LoomDtype.UTF8),)
    catalog.update_schema(TableRef("staging.out"), evolved)
    assert len(catalog.schema(TableRef("staging.out"))) == 4  # type: ignore[arg-type]
