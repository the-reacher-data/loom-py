"""Tests for IntoTable and IntoFile target declarations."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from loom.etl import col
from loom.etl.io._format import Format
from loom.etl.io.target import IntoFile, IntoTable, IntoTemp, SchemaMode
from loom.etl.io.target._file import FileSpec
from loom.etl.io.target._table import (
    AppendSpec,
    ReplacePartitionsSpec,
    ReplaceSpec,
    ReplaceWhereSpec,
    UpsertSpec,
)
from loom.etl.io.target._temp import TempFanInSpec, TempSpec
from loom.etl.pipeline._proxy import params
from loom.etl.schema._table import TableRef
from loom.etl.temp._scope import TempScope


class TestIntoTableModes:
    @pytest.mark.parametrize(
        "build,expected_type,expected_partitions,expect_predicate",
        [
            (lambda t: t, ReplaceSpec, (), False),
            (lambda t: t.append(), AppendSpec, (), False),
            (lambda t: t.replace(), ReplaceSpec, (), False),
            (
                lambda t: t.replace_partitions("year", "month"),
                ReplacePartitionsSpec,
                ("year", "month"),
                False,
            ),
            (
                lambda t: t.replace_partitions(
                    values={"year": params.run_date.year, "month": params.run_date.month}
                ),
                ReplaceWhereSpec,
                (),  # columns encoded in predicate, not in partition_cols
                True,
            ),
            (
                lambda t: t.replace_where(col("date") >= params.run_date),
                ReplaceWhereSpec,
                (),
                True,
            ),
            (
                lambda t: t.upsert(keys=("order_id",)),
                UpsertSpec,
                (),
                False,
            ),
        ],
    )
    def test_into_table_mode_contract(
        self,
        build: Callable[[IntoTable], IntoTable],
        expected_type: type,
        expected_partitions: tuple[str, ...],
        expect_predicate: bool,
    ) -> None:
        spec = build(IntoTable("staging.orders"))._to_spec()
        assert isinstance(spec, expected_type)
        assert getattr(spec, "partition_cols", ()) == expected_partitions
        assert (getattr(spec, "replace_predicate", None) is not None) is expect_predicate
        if isinstance(spec, UpsertSpec):
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
        assert isinstance(base._to_spec(), ReplaceSpec)

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

    @pytest.mark.parametrize("schema", [SchemaMode.STRICT, SchemaMode.EVOLVE, SchemaMode.OVERWRITE])
    def test_replace_propagates_schema_mode(self, schema: SchemaMode) -> None:
        spec = IntoTable("staging.orders").replace(schema=schema)._to_spec()
        assert isinstance(spec, ReplaceSpec)
        assert spec.schema_mode is schema

    def test_upsert_propagates_include_exclude_and_partitions(self) -> None:
        spec = (
            IntoTable("staging.orders")
            .upsert(
                keys=("order_id",),
                partition_cols=("year", "month"),
                exclude=("created_at",),
                include=("status",),
            )
            ._to_spec()
        )
        assert isinstance(spec, UpsertSpec)
        assert spec.upsert_keys == ("order_id",)
        assert spec.partition_cols == ("year", "month")
        assert spec.upsert_exclude == ("created_at",)
        assert spec.upsert_include == ("status",)

    def test_repr_includes_mode(self) -> None:
        repr_value = repr(IntoTable("staging.orders").append())
        assert "IntoTable('staging.orders'" in repr_value
        assert "append" in repr_value


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
    def test_into_file_stores_path_and_format(
        self,
        target: IntoFile,
        expected_path: str,
        expected_format: Format,
    ) -> None:
        spec = target._to_spec()
        assert isinstance(spec, FileSpec)
        assert spec.path == expected_path
        assert spec.format is expected_format

    def test_repr_includes_path_and_format(self) -> None:
        repr_value = repr(IntoFile("s3://out/report.xlsx", format=Format.XLSX))
        assert "s3://out/report.xlsx" in repr_value
        assert "xlsx" in repr_value


class TestIntoTemp:
    @pytest.mark.parametrize(
        "append,expected_type",
        [
            (False, TempSpec),
            (True, TempFanInSpec),
        ],
    )
    def test_to_spec_selects_variant(self, append: bool, expected_type: type[Any]) -> None:
        spec = IntoTemp("normalized", scope=TempScope.CORRELATION, append=append)._to_spec()
        assert isinstance(spec, expected_type)
        assert spec.temp_name == "normalized"
        assert spec.temp_scope is TempScope.CORRELATION

    def test_exposes_properties(self) -> None:
        target = IntoTemp("parts", scope=TempScope.RUN, append=True)
        assert target.temp_name == "parts"
        assert target.scope is TempScope.RUN
        assert target.append is True

    def test_repr_includes_name_scope_and_append(self) -> None:
        repr_value = repr(IntoTemp("parts", scope=TempScope.RUN, append=True))
        assert "IntoTemp('parts'" in repr_value
        assert "append=True" in repr_value
