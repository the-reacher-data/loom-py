"""Integration tests for DeltaCatalog, PolarsDeltaReader, PolarsDeltaWriter."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from deltalake import DeltaTable, write_deltalake

from loom.etl._format import Format
from loom.etl._schema import ColumnSchema, LoomDtype
from loom.etl._source import SourceKind, SourceSpec
from loom.etl._table import TableRef
from loom.etl._target import SchemaMode, TargetSpec, WriteMode
from loom.etl.backends.polars import DeltaCatalog, PolarsDeltaReader, PolarsDeltaWriter
from loom.etl.backends.polars._schema import SchemaError, SchemaNotFoundError

from .conftest import table_path


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
