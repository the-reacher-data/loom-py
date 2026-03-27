"""Tests for IntoTable and IntoFile target declarations."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from loom.etl import col
from loom.etl.io._format import Format
from loom.etl.io._target import IntoFile, IntoTable, WriteMode
from loom.etl.model._proxy import params
from loom.etl.schema._table import TableRef


class TestIntoTableModes:
    @pytest.mark.parametrize(
        "build,expected_mode,expected_partitions,expect_predicate",
        [
            (lambda t: t, WriteMode.REPLACE, (), False),
            (lambda t: t.append(), WriteMode.APPEND, (), False),
            (lambda t: t.replace(), WriteMode.REPLACE, (), False),
            (
                lambda t: t.replace_partitions("year", "month"),
                WriteMode.REPLACE_PARTITIONS,
                ("year", "month"),
                False,
            ),
            (
                lambda t: t.replace_partitions(
                    values={"year": params.run_date.year, "month": params.run_date.month}
                ),
                WriteMode.REPLACE_WHERE,
                ("year", "month"),
                True,
            ),
            (
                lambda t: t.replace_where(col("date") >= params.run_date),
                WriteMode.REPLACE_WHERE,
                (),
                True,
            ),
            (
                lambda t: t.upsert(keys=("order_id",)),
                WriteMode.UPSERT,
                (),
                False,
            ),
        ],
    )
    def test_into_table_mode_contract(
        self,
        build: Callable[[IntoTable], IntoTable],
        expected_mode: WriteMode,
        expected_partitions: tuple[str, ...],
        expect_predicate: bool,
    ) -> None:
        spec = build(IntoTable("staging.orders"))._to_spec()
        assert spec.mode is expected_mode
        assert spec.partition_cols == expected_partitions
        assert (spec.replace_predicate is not None) is expect_predicate
        if expected_mode is WriteMode.UPSERT:
            assert spec.upsert_keys == ("order_id",)

    @pytest.mark.parametrize(
        "call",
        [
            lambda t: t.replace_partitions("year", values={"year": params.run_date.year}),
            lambda t: t.replace_partitions(),
        ],
    )
    def test_replace_partitions_raises_on_invalid_args(
        self, call: Callable[[IntoTable], Any]
    ) -> None:
        with pytest.raises(ValueError):
            call(IntoTable("staging.orders"))

    def test_write_methods_return_new_instance(self) -> None:
        base = IntoTable("staging.orders")
        appended = base.append()
        assert appended is not base
        assert base._to_spec().mode is WriteMode.REPLACE

    @pytest.mark.parametrize(
        "table_ref,expected",
        [
            ("staging.orders", TableRef("staging.orders")),
            (TableRef("staging.orders"), TableRef("staging.orders")),
        ],
    )
    def test_table_ref_normalization(self, table_ref: str | TableRef, expected: TableRef) -> None:
        spec = IntoTable(table_ref)._to_spec()
        assert spec.table_ref == expected
        assert spec.format is Format.DELTA


class TestIntoFile:
    @pytest.mark.parametrize(
        "target,expected_path,expected_format",
        [
            (
                IntoFile("s3://exports/report_{run_date}.csv", format=Format.CSV),
                "s3://exports/report_{run_date}.csv",
                Format.CSV,
            ),
            (
                IntoFile("s3://out/report.xlsx", format=Format.XLSX),
                "s3://out/report.xlsx",
                Format.XLSX,
            ),
        ],
    )
    def test_into_file_stores_path_format_and_mode(
        self,
        target: IntoFile,
        expected_path: str,
        expected_format: Format,
    ) -> None:
        spec = target._to_spec()
        assert spec.path == expected_path
        assert spec.format is expected_format
        assert spec.mode is WriteMode.REPLACE
