"""Tests for IntoTable and IntoFile target declarations."""

from __future__ import annotations

from loom.etl._format import Format
from loom.etl._proxy import params
from loom.etl._table import TableRef
from loom.etl._target import IntoFile, IntoTable, WriteMode


def test_into_table_default_mode_is_replace() -> None:
    t = IntoTable("staging.orders")
    assert t._to_spec().mode is WriteMode.REPLACE


def test_into_table_append() -> None:
    spec = IntoTable("staging.orders").append()._to_spec()
    assert spec.mode is WriteMode.APPEND


def test_into_table_replace() -> None:
    spec = IntoTable("staging.orders").replace()._to_spec()
    assert spec.mode is WriteMode.REPLACE


def test_into_table_partition_replace() -> None:
    by_expr = params.run_date
    spec = IntoTable("staging.orders").partition_replace(by=by_expr)._to_spec()
    assert spec.mode is WriteMode.PARTITION_REPLACE
    assert spec.partition_by is by_expr


def test_into_table_upsert() -> None:
    spec = IntoTable("staging.orders").upsert(keys=("order_id",))._to_spec()
    assert spec.mode is WriteMode.UPSERT
    assert spec.upsert_keys == ("order_id",)


def test_into_table_write_methods_return_new_instance() -> None:
    base = IntoTable("staging.orders")
    appended = base.append()
    assert appended is not base
    assert base._to_spec().mode is WriteMode.REPLACE  # original unchanged


def test_into_table_str_ref_normalized() -> None:
    spec = IntoTable("staging.orders")._to_spec()
    assert spec.table_ref == TableRef("staging.orders")


def test_into_table_table_ref_accepted() -> None:
    ref = TableRef("staging.orders")
    spec = IntoTable(ref)._to_spec()
    assert spec.table_ref == ref


def test_into_table_format_is_delta() -> None:
    spec = IntoTable("staging.orders")._to_spec()
    assert spec.format is Format.DELTA


def test_into_file_stores_path_and_format() -> None:
    t = IntoFile("s3://exports/report_{run_date}.csv", format=Format.CSV)
    spec = t._to_spec()
    assert spec.path == "s3://exports/report_{run_date}.csv"
    assert spec.format is Format.CSV
    assert spec.mode is WriteMode.REPLACE


def test_into_file_xlsx() -> None:
    spec = IntoFile("s3://out/report.xlsx", format=Format.XLSX)._to_spec()
    assert spec.format is Format.XLSX
