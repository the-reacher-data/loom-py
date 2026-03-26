"""Tests for with_schema() / with_options() on FromTable, FromFile and IntoFile."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from loom.etl import (
    CsvReadOptions,
    CsvWriteOptions,
    ExcelReadOptions,
    Format,
    FromFile,
    FromTable,
    IntoFile,
    JsonReadOptions,
    ParquetReadOptions,
    ParquetWriteOptions,
)
from loom.etl._schema import ColumnSchema, LoomDtype
from loom.etl._source import SourceKind, SourceSpec

SCHEMA = (
    ColumnSchema("id", LoomDtype.INT64, nullable=False),
    ColumnSchema("amount", LoomDtype.FLOAT64),
)
ReadOptions = CsvReadOptions | JsonReadOptions | ExcelReadOptions | ParquetReadOptions
WriteOptions = CsvWriteOptions | ParquetWriteOptions


class TestFromTableWithSchema:
    def test_with_schema_sets_schema_and_preserves_ref(self) -> None:
        spec = FromTable("raw.orders").with_schema(SCHEMA)._to_spec("orders")
        assert spec.schema == SCHEMA
        assert spec.table_ref is not None
        assert spec.table_ref.ref == "raw.orders"

    def test_with_schema_immutable_original_unchanged(self) -> None:
        original = FromTable("raw.orders")
        _ = original.with_schema(SCHEMA)
        assert original._to_spec("orders").schema == ()

    def test_with_schema_preserves_predicates(self) -> None:
        from loom.etl._table import col

        enriched = FromTable("raw.orders").where(col("year") == 2024).with_schema(SCHEMA)
        assert len(enriched.predicates) == 1
        assert enriched._to_spec("orders").schema == SCHEMA


class TestFromFileSchemaAndOptions:
    def test_with_schema_sets_spec_schema(self) -> None:
        spec = (
            FromFile("s3://raw/events.json", format=Format.JSON)
            .with_schema(SCHEMA)
            ._to_spec("events")
        )
        assert spec.schema == SCHEMA

    def test_with_schema_immutable_original_unchanged(self) -> None:
        original = FromFile("s3://raw/events.json", format=Format.JSON)
        _ = original.with_schema(SCHEMA)
        assert original._to_spec("events").schema == ()

    @pytest.mark.parametrize(
        "source,options,checker",
        [
            (
                lambda: FromFile("s3://raw/export.csv", format=Format.CSV),
                CsvReadOptions(separator=";", has_header=False),
                lambda ro: (
                    isinstance(ro, CsvReadOptions)
                    and ro.separator == ";"
                    and ro.has_header is False
                ),
            ),
            (
                lambda: FromFile("s3://raw/events.json", format=Format.JSON),
                JsonReadOptions(infer_schema_length=None),
                lambda ro: isinstance(ro, JsonReadOptions) and ro.infer_schema_length is None,
            ),
            (
                lambda: FromFile("s3://raw/report.xlsx", format=Format.XLSX),
                ExcelReadOptions(sheet_name="Data", has_header=True),
                lambda ro: isinstance(ro, ExcelReadOptions) and ro.sheet_name == "Data",
            ),
            (
                lambda: FromFile("s3://raw/data.parquet", format=Format.PARQUET),
                ParquetReadOptions(),
                lambda ro: isinstance(ro, ParquetReadOptions),
            ),
        ],
    )
    def test_with_options_supports_read_formats(
        self,
        source: Callable[[], FromFile],
        options: ReadOptions,
        checker: Callable[[Any], bool],
    ) -> None:
        spec = source().with_options(options)._to_spec("data")
        assert checker(spec.read_options)

    def test_from_file_chaining_options_and_schema(self) -> None:
        spec = (
            FromFile("s3://raw/pipe.csv", format=Format.CSV)
            .with_options(CsvReadOptions(separator="|"))
            .with_schema(SCHEMA)
            ._to_spec("pipe")
        )
        assert isinstance(spec.read_options, CsvReadOptions)
        assert spec.read_options.separator == "|"
        assert spec.schema == SCHEMA

    def test_with_options_immutable_original_unchanged(self) -> None:
        original = FromFile("s3://raw/export.csv", format=Format.CSV)
        _ = original.with_options(CsvReadOptions(separator=";"))
        assert original._to_spec("report").read_options is None


class TestIntoFileWithOptions:
    @pytest.mark.parametrize(
        "target,options,checker",
        [
            (
                lambda: IntoFile("s3://exports/out.csv", format=Format.CSV),
                CsvWriteOptions(separator="\t", has_header=False),
                lambda wo: (
                    isinstance(wo, CsvWriteOptions)
                    and wo.separator == "\t"
                    and wo.has_header is False
                ),
            ),
            (
                lambda: IntoFile("s3://exports/out.parquet", format=Format.PARQUET),
                ParquetWriteOptions(compression="zstd"),
                lambda wo: isinstance(wo, ParquetWriteOptions) and wo.compression == "zstd",
            ),
        ],
    )
    def test_with_options_supports_write_formats(
        self,
        target: Callable[[], IntoFile],
        options: WriteOptions,
        checker: Callable[[Any], bool],
    ) -> None:
        spec = target().with_options(options)._to_spec()
        assert checker(spec.write_options)

    def test_with_options_immutable_original_unchanged(self) -> None:
        original = IntoFile("s3://exports/out.csv", format=Format.CSV)
        _ = original.with_options(CsvWriteOptions(separator=";"))
        assert original._to_spec().write_options is None


class TestSpecConsistency:
    @pytest.mark.parametrize(
        "spec_factory,expected_kind",
        [
            (
                lambda: FromFile("s3://raw/data.csv", format=Format.CSV)._to_spec("data"),
                SourceKind.FILE,
            ),
            (lambda: FromTable("raw.orders")._to_spec("orders"), SourceKind.TABLE),
        ],
    )
    def test_source_kind_is_consistent(
        self,
        spec_factory: Callable[[], SourceSpec],
        expected_kind: SourceKind,
    ) -> None:
        assert spec_factory().kind is expected_kind

    def test_from_file_no_options_by_default(self) -> None:
        spec = FromFile("s3://raw/data.csv", format=Format.CSV)._to_spec("data")
        assert spec.read_options is None
        assert spec.schema == ()
