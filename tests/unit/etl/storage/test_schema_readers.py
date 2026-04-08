"""Unit tests for physical schema readers."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from deltalake import write_deltalake

from loom.etl.schema._table import TableRef
from loom.etl.storage._locator import TableLocation
from loom.etl.storage.route import CatalogTarget, PathTarget
from loom.etl.storage.schema.delta import DeltaSchemaReader
from loom.etl.storage.schema.model import PolarsPhysicalSchema


def _path_target(uri: str) -> PathTarget:
    return PathTarget(
        logical_ref=TableRef("staging.orders"),
        location=TableLocation(uri=uri),
    )


def test_delta_schema_reader_returns_none_for_missing_table(tmp_path: Path) -> None:
    reader = DeltaSchemaReader()

    schema = reader.read_schema(_path_target(str(tmp_path / "missing")))

    assert schema is None


def test_delta_schema_reader_reads_columns_and_partitions(tmp_path: Path) -> None:
    table_path = tmp_path / "staging" / "orders"
    table_path.mkdir(parents=True, exist_ok=True)
    write_deltalake(
        str(table_path),
        pl.DataFrame({"id": [1, 2], "year": [2024, 2024], "amount": [10.0, 20.0]}).to_arrow(),
        mode="overwrite",
        partition_by=["year"],
    )

    reader = DeltaSchemaReader()
    schema = reader.read_schema(_path_target(str(table_path)))

    assert schema is not None
    assert isinstance(schema, PolarsPhysicalSchema)
    assert tuple(schema.schema.names()) == ("id", "year", "amount")
    assert schema.schema["id"] == pl.Int64
    assert schema.schema["year"] == pl.Int64
    assert schema.schema["amount"] == pl.Float64
    assert schema.partition_columns == ("year",)


def test_delta_schema_reader_rejects_catalog_target() -> None:
    reader = DeltaSchemaReader()
    target = CatalogTarget(
        logical_ref=TableRef("raw.orders"),
        catalog_ref=TableRef("main.raw.orders"),
    )

    with pytest.raises(ValueError, match="path targets"):
        reader.read_schema(target)
