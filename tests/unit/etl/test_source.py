"""Tests for FromTable, FromFile, Sources, SourceSet."""

from __future__ import annotations

import pytest

from loom.etl._format import Format
from loom.etl._proxy import params
from loom.etl._source import FromFile, FromTable, SourceKind, Sources, SourceSet
from loom.etl._table import TableRef, col


def test_from_table_str_ref_normalized_to_table_ref() -> None:
    src = FromTable("raw.orders")
    assert isinstance(src.table_ref, TableRef)
    assert src.table_ref.ref == "raw.orders"


def test_from_table_accepts_table_ref() -> None:
    ref = TableRef("raw.orders")
    src = FromTable(ref)
    assert src.table_ref is ref


def test_from_table_default_no_predicates() -> None:
    src = FromTable("raw.orders")
    assert src.predicates == ()


def test_from_table_where_returns_new_instance() -> None:
    src = FromTable("raw.orders")
    pred = col("year") == 2024
    filtered = src.where(pred)
    assert filtered is not src
    assert len(filtered.predicates) == 1
    assert filtered.predicates[0] is pred


def test_from_table_where_multiple_predicates() -> None:
    src = FromTable("raw.orders").where(
        col("year") == params.run_date.year,
        col("month") == params.run_date.month,
    )
    assert len(src.predicates) == 2


def test_from_table_to_spec_table_kind() -> None:
    src = FromTable("raw.orders")
    spec = src._to_spec("orders")
    assert spec.alias == "orders"
    assert spec.kind is SourceKind.TABLE
    assert spec.format is Format.DELTA
    assert spec.table_ref == TableRef("raw.orders")


def test_from_file_stores_path_and_format() -> None:
    src = FromFile("s3://raw/report_{run_date}.xlsx", format=Format.XLSX)
    assert src.path == "s3://raw/report_{run_date}.xlsx"
    assert src.format is Format.XLSX


def test_from_file_to_spec_file_kind() -> None:
    src = FromFile("s3://raw/data.csv", format=Format.CSV)
    spec = src._to_spec("data")
    assert spec.alias == "data"
    assert spec.kind is SourceKind.FILE
    assert spec.format is Format.CSV
    assert spec.path == "s3://raw/data.csv"


def test_sources_aliases_in_order() -> None:
    s = Sources(orders=FromTable("raw.orders"), customers=FromTable("raw.customers"))
    assert s.aliases == ("orders", "customers")


def test_sources_to_specs_maps_aliases() -> None:
    s = Sources(
        orders=FromTable("raw.orders"),
        customers=FromTable("raw.customers"),
    )
    specs = s._to_specs()
    assert len(specs) == 2
    assert specs[0].alias == "orders"
    assert specs[1].alias == "customers"


def test_source_set_subclass_collects_sources() -> None:
    class OrderSources(SourceSet[object]):
        orders = FromTable("raw.orders")
        customers = FromTable("raw.customers")

    specs = OrderSources()._to_specs()
    aliases = {s.alias for s in specs}
    assert aliases == {"orders", "customers"}


def test_source_set_extended_adds_extra() -> None:
    class Base(SourceSet[object]):
        orders = FromTable("raw.orders")

    extended = Base.extended(customers=FromTable("raw.customers"))
    specs = extended._to_specs()
    aliases = {s.alias for s in specs}
    assert aliases == {"orders", "customers"}


def test_source_set_extended_conflict_raises() -> None:
    class Base(SourceSet[object]):
        orders = FromTable("raw.orders")

    with pytest.raises(ValueError, match="conflicting source names"):
        Base.extended(orders=FromTable("raw.orders_v2"))


def test_source_set_extended_returns_new_instance() -> None:
    class Base(SourceSet[object]):
        orders = FromTable("raw.orders")

    extended = Base.extended(customers=FromTable("raw.customers"))
    assert extended is not Base


# ---------------------------------------------------------------------------
# FromTable.columns()
# ---------------------------------------------------------------------------


def test_from_table_columns_returns_new_instance() -> None:
    src = FromTable("raw.orders")
    projected = src.columns("id", "amount")
    assert projected is not src


def test_from_table_columns_stored_in_spec() -> None:
    spec = FromTable("raw.orders").columns("id", "amount", "status")._to_spec("orders")
    assert spec.columns == ("id", "amount", "status")


def test_from_table_default_columns_is_empty() -> None:
    spec = FromTable("raw.orders")._to_spec("orders")
    assert spec.columns == ()


def test_from_table_columns_empty_raises() -> None:
    with pytest.raises(ValueError, match="at least one"):
        FromTable("raw.orders").columns()


def test_from_table_columns_preserves_predicates() -> None:
    pred = col("year") == 2024
    src = FromTable("raw.orders").where(pred).columns("id", "amount")
    spec = src._to_spec("orders")
    assert len(spec.predicates) == 1
    assert spec.columns == ("id", "amount")


def test_from_table_columns_preserves_schema() -> None:
    from loom.etl._schema import ColumnSchema, LoomDtype

    schema = (ColumnSchema("id", LoomDtype.INT64),)
    src = FromTable("raw.orders").with_schema(schema).columns("id")
    spec = src._to_spec("orders")
    assert spec.schema == schema
    assert spec.columns == ("id",)


def test_from_table_where_preserves_columns() -> None:
    pred = col("year") == 2024
    src = FromTable("raw.orders").columns("id", "amount").where(pred)
    spec = src._to_spec("orders")
    assert spec.columns == ("id", "amount")
    assert len(spec.predicates) == 1


# ---------------------------------------------------------------------------
# FromFile.columns()
# ---------------------------------------------------------------------------


def test_from_file_columns_stored_in_spec() -> None:
    spec = (
        FromFile("s3://raw/data.parquet", format=Format.PARQUET)
        .columns("order_id", "amount")
        ._to_spec("data")
    )
    assert spec.columns == ("order_id", "amount")


def test_from_file_default_columns_is_empty() -> None:
    spec = FromFile("s3://raw/data.csv", format=Format.CSV)._to_spec("data")
    assert spec.columns == ()


def test_from_file_columns_empty_raises() -> None:
    with pytest.raises(ValueError, match="at least one"):
        FromFile("s3://raw/data.csv", format=Format.CSV).columns()


def test_from_file_columns_preserved_by_with_schema() -> None:
    from loom.etl._schema import ColumnSchema, LoomDtype

    schema = (ColumnSchema("id", LoomDtype.INT64),)
    src = (
        FromFile("s3://raw/data.csv", format=Format.CSV).columns("id", "amount").with_schema(schema)
    )
    spec = src._to_spec("data")
    assert spec.columns == ("id", "amount")
    assert spec.schema == schema
