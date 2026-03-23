"""Tests for IntoTable and IntoFile target declarations."""

from __future__ import annotations

import pytest

from loom.etl import col
from loom.etl._format import Format
from loom.etl._proxy import params
from loom.etl._table import TableRef
from loom.etl._target import IntoFile, IntoTable, WriteMode


@pytest.mark.parametrize(
    "build,expected_mode",
    [
        (lambda t: t, WriteMode.REPLACE),
        (lambda t: t.append(), WriteMode.APPEND),
        (lambda t: t.replace(), WriteMode.REPLACE),
    ],
)
def test_into_table_write_mode(build: object, expected_mode: WriteMode) -> None:
    spec = build(IntoTable("staging.orders"))._to_spec()  # type: ignore[operator]
    assert spec.mode is expected_mode


def test_into_table_replace_partitions_dynamic() -> None:
    spec = IntoTable("staging.orders").replace_partitions("year", "month")._to_spec()
    assert spec.mode is WriteMode.REPLACE_PARTITIONS
    assert spec.partition_cols == ("year", "month")


def test_into_table_replace_partitions_from_params() -> None:
    spec = (
        IntoTable("staging.orders")
        .replace_partitions(values={"year": params.run_date.year, "month": params.run_date.month})
        ._to_spec()
    )
    assert spec.mode is WriteMode.REPLACE_WHERE
    assert spec.partition_cols == ("year", "month")
    assert spec.replace_predicate is not None


def test_into_table_replace_where() -> None:
    pred = col("date") >= params.run_date
    spec = IntoTable("staging.orders").replace_where(pred)._to_spec()
    assert spec.mode is WriteMode.REPLACE_WHERE
    assert spec.replace_predicate is pred


@pytest.mark.parametrize(
    "call",
    [
        lambda t: t.replace_partitions("year", values={"year": params.run_date.year}),
        lambda t: t.replace_partitions(),
    ],
)
def test_replace_partitions_raises_on_invalid_args(call: object) -> None:
    with pytest.raises(ValueError):
        call(IntoTable("staging.orders"))  # type: ignore[operator]


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
