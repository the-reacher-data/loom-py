"""Integration tests for DeltaCatalog, PolarsDeltaReader, PolarsDeltaWriter."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest
from deltalake import DeltaTable, write_deltalake

from loom.etl import ETLParams, col
from loom.etl._format import Format
from loom.etl._proxy import params as p
from loom.etl._schema import ColumnSchema, LoomDtype
from loom.etl._source import SourceKind, SourceSpec
from loom.etl._table import TableRef
from loom.etl._target import IntoTable, SchemaMode, TargetSpec, WriteMode
from loom.etl.backends.polars import DeltaCatalog, PolarsDeltaReader, PolarsDeltaWriter
from loom.etl.backends.polars._schema import SchemaError, SchemaNotFoundError

from .conftest import table_path


class _DateParams(ETLParams):
    run_date: date


def _file_source_spec() -> SourceSpec:
    return SourceSpec(
        alias="data", kind=SourceKind.FILE, format=Format.CSV, path="s3://bucket/data.csv"
    )


def _file_target_spec() -> TargetSpec:
    return TargetSpec(mode=WriteMode.REPLACE, format=Format.CSV, path="s3://bucket/out.csv")


def _seed(root: Path, ref: str, data: pl.DataFrame) -> None:
    path = table_path(root, TableRef(ref))
    path.mkdir(parents=True, exist_ok=True)
    write_deltalake(str(path), data.to_arrow(), mode="overwrite")


def _spec(
    ref: str, mode: WriteMode = WriteMode.REPLACE, schema_mode: SchemaMode = SchemaMode.STRICT
) -> TargetSpec:
    return TargetSpec(
        mode=mode,
        format=Format.DELTA,
        schema_mode=schema_mode,
        table_ref=TableRef(ref),
    )


def _source_spec(ref: str) -> SourceSpec:
    return SourceSpec(
        alias="data", kind=SourceKind.TABLE, format=Format.DELTA, table_ref=TableRef(ref)
    )


def test_catalog_exists_false_for_missing_table(tmp_path: Path) -> None:
    catalog = DeltaCatalog(tmp_path)
    assert not catalog.exists(TableRef("raw.orders"))


def test_catalog_exists_true_after_write(tmp_path: Path) -> None:
    _seed(tmp_path, "raw.orders", pl.DataFrame({"id": [1]}))
    catalog = DeltaCatalog(tmp_path)
    assert catalog.exists(TableRef("raw.orders"))


def test_catalog_schema_returns_none_for_missing_table(tmp_path: Path) -> None:
    assert DeltaCatalog(tmp_path).schema(TableRef("raw.orders")) is None


def test_catalog_schema_reflects_written_table(tmp_path: Path) -> None:
    _seed(tmp_path, "raw.orders", pl.DataFrame({"id": [1], "amount": [1.0]}))
    schema = DeltaCatalog(tmp_path).schema(TableRef("raw.orders"))
    assert schema is not None
    names = [col.name for col in schema]
    assert names == ["id", "amount"]
    assert schema[0].dtype is LoomDtype.INT64
    assert schema[1].dtype is LoomDtype.FLOAT64


def test_catalog_columns_derived_from_schema(tmp_path: Path) -> None:
    _seed(tmp_path, "raw.orders", pl.DataFrame({"id": [1], "amount": [1.0]}))
    cols = DeltaCatalog(tmp_path).columns(TableRef("raw.orders"))
    assert cols == ("id", "amount")


def test_catalog_update_schema_is_noop(tmp_path: Path) -> None:
    """DeltaCatalog ignores update_schema — Delta log is the source of truth."""
    _seed(tmp_path, "raw.orders", pl.DataFrame({"id": [1]}))
    catalog = DeltaCatalog(tmp_path)
    new_schema = (ColumnSchema("id", LoomDtype.INT64), ColumnSchema("fake", LoomDtype.UTF8))
    catalog.update_schema(TableRef("raw.orders"), new_schema)
    # Schema still reflects what Delta has on disk, not the injected value
    schema = catalog.schema(TableRef("raw.orders"))
    assert schema is not None
    assert len(schema) == 1


def test_reader_returns_lazy_frame(tmp_path: Path) -> None:
    _seed(tmp_path, "raw.orders", pl.DataFrame({"id": [1, 2]}))
    reader = PolarsDeltaReader(tmp_path)
    result = reader.read(_source_spec("raw.orders"), None)
    assert isinstance(result, pl.LazyFrame)


def test_reader_reads_correct_data(tmp_path: Path) -> None:
    data = pl.DataFrame({"id": [1, 2, 3], "v": [10, 20, 30]})
    _seed(tmp_path, "raw.events", data)
    result = PolarsDeltaReader(tmp_path).read(_source_spec("raw.events"), None).collect()
    assert result["id"].to_list() == [1, 2, 3]
    assert result["v"].to_list() == [10, 20, 30]


def test_writer_strict_passes_with_matching_frame(tmp_path: Path) -> None:
    _seed(tmp_path, "staging.out", pl.DataFrame({"id": [0], "v": [0.0]}))
    catalog = DeltaCatalog(tmp_path)
    writer = PolarsDeltaWriter(tmp_path, catalog)
    frame = pl.DataFrame({"id": [1], "v": [1.0]}).lazy()
    writer.write(frame, _spec("staging.out", schema_mode=SchemaMode.STRICT), None)
    result = pl.from_arrow(
        __import__("deltalake")
        .DeltaTable(str(table_path(tmp_path, TableRef("staging.out"))))
        .to_pyarrow_table()
    )
    assert result["id"].to_list() == [1]


def test_writer_strict_fails_on_extra_column(tmp_path: Path) -> None:
    _seed(tmp_path, "staging.out", pl.DataFrame({"id": [0]}))
    catalog = DeltaCatalog(tmp_path)
    writer = PolarsDeltaWriter(tmp_path, catalog)
    frame = pl.DataFrame({"id": [1], "extra": ["x"]}).lazy()
    with pytest.raises(SchemaError):
        writer.write(frame, _spec("staging.out", schema_mode=SchemaMode.STRICT), None)


def test_writer_raises_schema_not_found_when_table_missing(tmp_path: Path) -> None:
    catalog = DeltaCatalog(tmp_path)
    writer = PolarsDeltaWriter(tmp_path, catalog)
    frame = pl.DataFrame({"id": [1]}).lazy()
    with pytest.raises(SchemaNotFoundError):
        writer.write(frame, _spec("staging.new", schema_mode=SchemaMode.STRICT), None)


def test_writer_overwrite_creates_table_when_missing(tmp_path: Path) -> None:
    """OVERWRITE on a non-existent table creates it from the frame schema."""
    catalog = DeltaCatalog(tmp_path)
    writer = PolarsDeltaWriter(tmp_path, catalog)
    frame = pl.DataFrame({"id": [1, 2], "v": [10, 20]}).lazy()
    writer.write(frame, _spec("staging.new", schema_mode=SchemaMode.OVERWRITE), None)
    assert catalog.exists(TableRef("staging.new"))
    schema = catalog.schema(TableRef("staging.new"))
    assert schema is not None
    assert [col.name for col in schema] == ["id", "v"]


def test_writer_evolve_fills_missing_columns(tmp_path: Path) -> None:
    """EVOLVE: columns missing from frame are written as nulls."""
    _seed(tmp_path, "staging.out", pl.DataFrame({"id": [0], "label": ["x"]}))
    catalog = DeltaCatalog(tmp_path)
    writer = PolarsDeltaWriter(tmp_path, catalog)
    frame = pl.DataFrame({"id": [1]}).lazy()  # label missing
    writer.write(frame, _spec("staging.out", schema_mode=SchemaMode.EVOLVE), None)
    result = pl.from_arrow(
        DeltaTable(str(table_path(tmp_path, TableRef("staging.out")))).to_pyarrow_table()
    )
    assert "label" in result.columns
    written_rows = result.filter(pl.col("id") == 1)
    assert written_rows["label"][0] is None


def test_reader_raises_type_error_for_file_spec(tmp_path: Path) -> None:
    reader = PolarsDeltaReader(tmp_path)
    with pytest.raises(TypeError, match="FILE"):
        reader.read(_file_source_spec(), None)


def test_writer_raises_type_error_for_file_spec(tmp_path: Path) -> None:
    writer = PolarsDeltaWriter(tmp_path, DeltaCatalog(tmp_path))
    with pytest.raises(TypeError, match="FILE"):
        writer.write(pl.DataFrame({"id": [1]}).lazy(), _file_target_spec(), None)


def test_writer_append_adds_rows(tmp_path: Path) -> None:
    _seed(tmp_path, "staging.ledger", pl.DataFrame({"id": [0], "v": [0.0]}))
    catalog = DeltaCatalog(tmp_path)
    writer = PolarsDeltaWriter(tmp_path, catalog)
    frame = pl.DataFrame({"id": [1, 2], "v": [1.0, 2.0]}).lazy()
    writer.write(frame, _spec("staging.ledger", mode=WriteMode.APPEND), None)
    result = pl.from_arrow(
        DeltaTable(str(table_path(tmp_path, TableRef("staging.ledger")))).to_pyarrow_table()
    )
    assert result.height == 3


def test_writer_replace_partitions_overwrites_matching_partition(tmp_path: Path) -> None:
    initial = pl.DataFrame({"year": [2023, 2024], "v": [10, 20]})
    _seed(tmp_path, "staging.facts", initial)
    catalog = DeltaCatalog(tmp_path)
    writer = PolarsDeltaWriter(tmp_path, catalog)
    new_data = pl.DataFrame({"year": [2024], "v": [99]})
    spec = TargetSpec(
        mode=WriteMode.REPLACE_PARTITIONS,
        format=Format.DELTA,
        schema_mode=SchemaMode.STRICT,
        table_ref=TableRef("staging.facts"),
        partition_cols=("year",),
    )
    writer.write(new_data.lazy(), spec, None)
    result = pl.from_arrow(
        DeltaTable(str(table_path(tmp_path, TableRef("staging.facts")))).to_pyarrow_table()
    )
    assert result.filter(pl.col("year") == 2024)["v"].to_list() == [99]
    assert result.filter(pl.col("year") == 2023)["v"].to_list() == [10]


def test_writer_replace_partitions_with_string_values(tmp_path: Path) -> None:
    initial = pl.DataFrame({"region": ["us", "eu"], "v": [1, 2]})
    _seed(tmp_path, "staging.regions", initial)
    catalog = DeltaCatalog(tmp_path)
    writer = PolarsDeltaWriter(tmp_path, catalog)
    new_data = pl.DataFrame({"region": ["us"], "v": [99]})
    spec = TargetSpec(
        mode=WriteMode.REPLACE_PARTITIONS,
        format=Format.DELTA,
        schema_mode=SchemaMode.STRICT,
        table_ref=TableRef("staging.regions"),
        partition_cols=("region",),
    )
    writer.write(new_data.lazy(), spec, None)
    result = pl.from_arrow(
        DeltaTable(str(table_path(tmp_path, TableRef("staging.regions")))).to_pyarrow_table()
    )
    assert result.filter(pl.col("region") == "us")["v"].to_list() == [99]
    assert result.filter(pl.col("region") == "eu")["v"].to_list() == [2]


def test_writer_replace_partitions_with_bool_values(tmp_path: Path) -> None:
    initial = pl.DataFrame({"active": [True, False], "v": [1, 2]})
    _seed(tmp_path, "staging.flags", initial)
    catalog = DeltaCatalog(tmp_path)
    writer = PolarsDeltaWriter(tmp_path, catalog)
    new_data = pl.DataFrame({"active": [True], "v": [99]})
    spec = TargetSpec(
        mode=WriteMode.REPLACE_PARTITIONS,
        format=Format.DELTA,
        schema_mode=SchemaMode.STRICT,
        table_ref=TableRef("staging.flags"),
        partition_cols=("active",),
    )
    writer.write(new_data.lazy(), spec, None)
    result = pl.from_arrow(
        DeltaTable(str(table_path(tmp_path, TableRef("staging.flags")))).to_pyarrow_table()
    )
    assert result.filter(pl.col("active"))["v"].to_list() == [99]
    assert result.filter(~pl.col("active"))["v"].to_list() == [2]


def test_writer_replace_where_overwrites_matching_rows(tmp_path: Path) -> None:
    initial = pl.DataFrame({"year": [2023, 2024], "v": [10, 20]})
    _seed(tmp_path, "staging.yearly", initial)
    catalog = DeltaCatalog(tmp_path)
    writer = PolarsDeltaWriter(tmp_path, catalog)
    new_data = pl.DataFrame({"year": [2024], "v": [99]})
    pred = col("year") == p.run_date.year
    spec = IntoTable("staging.yearly").replace_where(pred)._to_spec()
    writer.write(new_data.lazy(), spec, _DateParams(run_date=date(2024, 1, 1)))
    result = pl.from_arrow(
        DeltaTable(str(table_path(tmp_path, TableRef("staging.yearly")))).to_pyarrow_table()
    )
    assert result.filter(pl.col("year") == 2024)["v"].to_list() == [99]
    assert result.filter(pl.col("year") == 2023)["v"].to_list() == [10]


def test_writer_updates_catalog_schema_after_write(tmp_path: Path) -> None:
    """Catalog schema reflects the written frame after a successful write."""
    _seed(tmp_path, "staging.out", pl.DataFrame({"id": [0]}))
    catalog = DeltaCatalog(tmp_path)
    writer = PolarsDeltaWriter(tmp_path, catalog)
    frame = pl.DataFrame({"id": [1]}).lazy()
    writer.write(frame, _spec("staging.out", schema_mode=SchemaMode.STRICT), None)
    schema = catalog.schema(TableRef("staging.out"))
    assert schema is not None
    assert schema[0].dtype is LoomDtype.INT64
