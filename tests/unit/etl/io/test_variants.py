"""Tests for TargetSpec variant structs — frozen dataclasses, field contracts."""

from __future__ import annotations

import pytest

from loom.etl import col
from loom.etl.io._format import Format
from loom.etl.io.target import SchemaMode, TargetSpec
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

_REF = TableRef("staging.out")
_PRED = col("year") == params.run_date.year


class TestTableVariants:
    def test_append_defaults(self) -> None:
        spec = AppendSpec(table_ref=_REF)
        assert spec.table_ref is _REF
        assert spec.schema_mode is SchemaMode.STRICT

    def test_replace_defaults(self) -> None:
        spec = ReplaceSpec(table_ref=_REF)
        assert spec.table_ref is _REF
        assert spec.schema_mode is SchemaMode.STRICT

    def test_replace_partitions_stores_cols(self) -> None:
        spec = ReplacePartitionsSpec(table_ref=_REF, partition_cols=("year", "month"))
        assert spec.partition_cols == ("year", "month")
        assert spec.schema_mode is SchemaMode.STRICT

    def test_replace_where_stores_predicate(self) -> None:
        spec = ReplaceWhereSpec(table_ref=_REF, replace_predicate=_PRED)
        assert spec.replace_predicate is _PRED

    def test_upsert_keys_and_defaults(self) -> None:
        spec = UpsertSpec(table_ref=_REF, upsert_keys=("id",))
        assert spec.upsert_keys == ("id",)
        assert spec.partition_cols == ()
        assert spec.upsert_exclude == ()
        assert spec.upsert_include == ()

    def test_upsert_include_exclude(self) -> None:
        spec = UpsertSpec(table_ref=_REF, upsert_keys=("id",), upsert_exclude=("created_at",))
        assert spec.upsert_exclude == ("created_at",)
        spec2 = UpsertSpec(table_ref=_REF, upsert_keys=("id",), upsert_include=("status",))
        assert spec2.upsert_include == ("status",)

    @pytest.mark.parametrize(
        "spec",
        [
            AppendSpec(table_ref=_REF),
            ReplaceSpec(table_ref=_REF),
            ReplacePartitionsSpec(table_ref=_REF, partition_cols=("year",)),
            ReplaceWhereSpec(table_ref=_REF, replace_predicate=_PRED),
            UpsertSpec(table_ref=_REF, upsert_keys=("id",)),
        ],
    )
    def test_table_variants_are_frozen(self, spec: object) -> None:
        with pytest.raises((AttributeError, TypeError)):
            spec.table_ref = TableRef("other.table")  # type: ignore[union-attr]

    @pytest.mark.parametrize(
        "spec",
        [
            AppendSpec(table_ref=_REF),
            ReplaceSpec(table_ref=_REF),
            ReplacePartitionsSpec(table_ref=_REF, partition_cols=("year",)),
            ReplaceWhereSpec(table_ref=_REF, replace_predicate=_PRED),
            UpsertSpec(table_ref=_REF, upsert_keys=("id",)),
            FileSpec(path="s3://bucket/out.csv", format=Format.CSV),
            TempSpec(temp_name="normalized", temp_scope=TempScope.RUN),
            TempFanInSpec(temp_name="parts", temp_scope=TempScope.RUN),
        ],
    )
    def test_all_variants_are_target_spec(self, spec: object) -> None:
        assert isinstance(spec, TargetSpec)  # type: ignore[arg-type]


class TestFileSpec:
    def test_stores_path_and_format(self) -> None:
        spec = FileSpec(path="s3://bucket/out.csv", format=Format.CSV)
        assert spec.path == "s3://bucket/out.csv"
        assert spec.format is Format.CSV
        assert spec.write_options is None

    def test_frozen(self) -> None:
        spec = FileSpec(path="out.csv", format=Format.CSV)
        with pytest.raises((AttributeError, TypeError)):
            spec.path = "other.csv"  # type: ignore[misc]


class TestTempVariants:
    def test_temp_spec(self) -> None:
        spec = TempSpec(temp_name="norm", temp_scope=TempScope.RUN)
        assert spec.temp_name == "norm"
        assert spec.temp_scope is TempScope.RUN

    def test_temp_fan_in_spec(self) -> None:
        spec = TempFanInSpec(temp_name="parts", temp_scope=TempScope.RUN)
        assert spec.temp_name == "parts"

    def test_temp_and_fan_in_are_distinct_types(self) -> None:
        assert type(TempSpec(temp_name="x", temp_scope=TempScope.RUN)) is not type(
            TempFanInSpec(temp_name="x", temp_scope=TempScope.RUN)
        )
