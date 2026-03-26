"""Tests for with_schema() / with_options() on FromTable, FromFile and IntoFile."""

from __future__ import annotations

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
from loom.etl._source import SourceKind

_SCHEMA = (
    ColumnSchema("id", LoomDtype.INT64, nullable=False),
    ColumnSchema("amount", LoomDtype.FLOAT64),
)


# ---------------------------------------------------------------------------
# FromTable.with_schema()
# ---------------------------------------------------------------------------


def test_from_table_with_schema_sets_spec_schema() -> None:
    spec = FromTable("raw.orders").with_schema(_SCHEMA)._to_spec("orders")
    assert spec.schema == _SCHEMA


def test_from_table_with_schema_immutable_original_unchanged() -> None:
    original = FromTable("raw.orders")
    _ = original.with_schema(_SCHEMA)
    assert original._to_spec("orders").schema == ()


def test_from_table_with_schema_preserves_predicates() -> None:
    from loom.etl._table import col

    base = FromTable("raw.orders").where(col("year") == 2024)
    enriched = base.with_schema(_SCHEMA)
    assert len(enriched.predicates) == 1
    assert enriched._to_spec("orders").schema == _SCHEMA


def test_from_table_with_schema_preserves_ref() -> None:
    spec = FromTable("raw.orders").with_schema(_SCHEMA)._to_spec("orders")
    assert spec.table_ref is not None
    assert spec.table_ref.ref == "raw.orders"


# ---------------------------------------------------------------------------
# FromFile.with_schema()
# ---------------------------------------------------------------------------


def test_from_file_with_schema_sets_spec_schema() -> None:
    spec = (
        FromFile("s3://raw/events.json", format=Format.JSON).with_schema(_SCHEMA)._to_spec("events")
    )
    assert spec.schema == _SCHEMA


def test_from_file_with_schema_immutable_original_unchanged() -> None:
    original = FromFile("s3://raw/events.json", format=Format.JSON)
    _ = original.with_schema(_SCHEMA)
    assert original._to_spec("events").schema == ()


# ---------------------------------------------------------------------------
# FromFile.with_options()
# ---------------------------------------------------------------------------


def test_from_file_with_options_csv() -> None:
    opts = CsvReadOptions(separator=";", has_header=False)
    spec = FromFile("s3://raw/export.csv", format=Format.CSV).with_options(opts)._to_spec("report")
    assert isinstance(spec.read_options, CsvReadOptions)
    assert spec.read_options.separator == ";"
    assert spec.read_options.has_header is False


def test_from_file_with_options_json() -> None:
    opts = JsonReadOptions(infer_schema_length=None)
    spec = (
        FromFile("s3://raw/events.json", format=Format.JSON).with_options(opts)._to_spec("events")
    )
    assert isinstance(spec.read_options, JsonReadOptions)
    assert spec.read_options.infer_schema_length is None


def test_from_file_with_options_excel() -> None:
    opts = ExcelReadOptions(sheet_name="Data", has_header=True)
    spec = (
        FromFile("s3://raw/report.xlsx", format=Format.XLSX).with_options(opts)._to_spec("report")
    )
    assert isinstance(spec.read_options, ExcelReadOptions)
    assert spec.read_options.sheet_name == "Data"


def test_from_file_with_options_parquet() -> None:
    opts = ParquetReadOptions()
    spec = (
        FromFile("s3://raw/data.parquet", format=Format.PARQUET).with_options(opts)._to_spec("data")
    )
    assert isinstance(spec.read_options, ParquetReadOptions)


def test_from_file_chaining_options_and_schema() -> None:
    opts = CsvReadOptions(separator="|")
    spec = (
        FromFile("s3://raw/pipe.csv", format=Format.CSV)
        .with_options(opts)
        .with_schema(_SCHEMA)
        ._to_spec("pipe")
    )
    assert spec.read_options is not None
    assert isinstance(spec.read_options, CsvReadOptions)
    assert spec.read_options.separator == "|"
    assert spec.schema == _SCHEMA


def test_from_file_with_options_immutable_original_unchanged() -> None:
    original = FromFile("s3://raw/export.csv", format=Format.CSV)
    _ = original.with_options(CsvReadOptions(separator=";"))
    assert original._to_spec("report").read_options is None


# ---------------------------------------------------------------------------
# IntoFile.with_options()
# ---------------------------------------------------------------------------


def test_into_file_with_options_csv() -> None:
    opts = CsvWriteOptions(separator="\t", has_header=False)
    spec = IntoFile("s3://exports/out.csv", format=Format.CSV).with_options(opts)._to_spec()
    assert isinstance(spec.write_options, CsvWriteOptions)
    assert spec.write_options.separator == "\t"
    assert spec.write_options.has_header is False


def test_into_file_with_options_parquet() -> None:
    opts = ParquetWriteOptions(compression="zstd")
    spec = IntoFile("s3://exports/out.parquet", format=Format.PARQUET).with_options(opts)._to_spec()
    assert isinstance(spec.write_options, ParquetWriteOptions)
    assert spec.write_options.compression == "zstd"


def test_into_file_with_options_immutable_original_unchanged() -> None:
    original = IntoFile("s3://exports/out.csv", format=Format.CSV)
    _ = original.with_options(CsvWriteOptions(separator=";"))
    assert original._to_spec().write_options is None


# ---------------------------------------------------------------------------
# SourceKind and spec consistency
# ---------------------------------------------------------------------------


def test_from_file_spec_kind_is_file() -> None:
    spec = FromFile("s3://raw/data.csv", format=Format.CSV)._to_spec("data")
    assert spec.kind is SourceKind.FILE


def test_from_table_spec_kind_is_table() -> None:
    spec = FromTable("raw.orders")._to_spec("orders")
    assert spec.kind is SourceKind.TABLE


def test_from_file_no_options_by_default() -> None:
    spec = FromFile("s3://raw/data.csv", format=Format.CSV)._to_spec("data")
    assert spec.read_options is None
    assert spec.schema == ()
