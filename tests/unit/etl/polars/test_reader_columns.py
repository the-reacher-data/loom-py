"""Integration tests for PolarsDeltaReader column projection.

Verifies that .columns() on FromTable and FromFile pushes column selection
down to the Parquet scanner — only declared columns are materialised.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
from deltalake import write_deltalake

from loom.etl.backends.polars import PolarsDeltaReader
from loom.etl.io._format import Format
from loom.etl.io._source import FromFile, FromTable, SourceKind, SourceSpec
from loom.etl.schema._table import TableRef

from .conftest import table_path

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_delta(root: Path, ref: str, data: pl.DataFrame) -> Path:
    path = table_path(root, TableRef(ref))
    path.mkdir(parents=True, exist_ok=True)
    write_deltalake(str(path), data.to_arrow(), mode="overwrite")
    return path


def _reader(root: Path) -> PolarsDeltaReader:
    return PolarsDeltaReader(str(root))


def _table_spec(ref: str, columns: tuple[str, ...] = ()) -> SourceSpec:
    return SourceSpec(
        alias="data",
        kind=SourceKind.TABLE,
        format=Format.DELTA,
        table_ref=TableRef(ref),
        columns=columns,
    )


# ---------------------------------------------------------------------------
# FromTable.columns() — column projection on Delta table
# ---------------------------------------------------------------------------


def test_reader_no_columns_returns_all(tmp_path: Path) -> None:
    data = pl.DataFrame({"id": [1, 2], "name": ["a", "b"], "amount": [10.0, 20.0]})
    _seed_delta(tmp_path, "raw.orders", data)

    spec = _table_spec("raw.orders")
    result = _reader(tmp_path).read(spec, None).collect()

    assert set(result.columns) == {"id", "name", "amount"}


def test_reader_columns_projects_subset(tmp_path: Path) -> None:
    data = pl.DataFrame({"id": [1, 2], "name": ["a", "b"], "amount": [10.0, 20.0]})
    _seed_delta(tmp_path, "raw.orders", data)

    spec = _table_spec("raw.orders", columns=("id", "amount"))
    result = _reader(tmp_path).read(spec, None).collect()

    assert set(result.columns) == {"id", "amount"}
    assert "name" not in result.columns


def test_reader_columns_single_column(tmp_path: Path) -> None:
    data = pl.DataFrame({"id": [1, 2], "value": [100, 200]})
    _seed_delta(tmp_path, "raw.metrics", data)

    spec = _table_spec("raw.metrics", columns=("id",))
    result = _reader(tmp_path).read(spec, None).collect()

    assert result.columns == ["id"]


def test_reader_columns_preserves_row_count(tmp_path: Path) -> None:
    data = pl.DataFrame({"id": [1, 2, 3], "x": [10, 20, 30], "y": [1, 2, 3]})
    _seed_delta(tmp_path, "raw.events", data)

    spec = _table_spec("raw.events", columns=("id", "x"))
    result = _reader(tmp_path).read(spec, None).collect()

    assert len(result) == 3
    assert result["id"].to_list() == [1, 2, 3]


def test_reader_columns_combined_with_predicate(tmp_path: Path) -> None:
    from loom.etl.schema._table import UnboundColumnRef
    from loom.etl.sql._predicate import EqPred

    data = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "year": [2023, 2024, 2024],
            "amount": [10.0, 20.0, 30.0],
        }
    )
    _seed_delta(tmp_path, "raw.orders", data)

    pred = EqPred(left=UnboundColumnRef("year"), right=2024)
    spec = SourceSpec(
        alias="orders",
        kind=SourceKind.TABLE,
        format=Format.DELTA,
        table_ref=TableRef("raw.orders"),
        predicates=(pred,),
        columns=("id", "amount"),
    )
    result = _reader(tmp_path).read(spec, None).collect()

    assert set(result.columns) == {"id", "amount"}
    assert result["id"].to_list() == [2, 3]


# ---------------------------------------------------------------------------
# FromTable.columns() builder — spec propagation
# ---------------------------------------------------------------------------


def test_from_table_columns_spec_propagation() -> None:
    spec = FromTable("raw.orders").columns("id", "status")._to_spec("orders")
    assert spec.columns == ("id", "status")


def test_from_table_columns_chained_with_where() -> None:
    from loom.etl.schema._table import col

    spec = (
        FromTable("raw.orders")
        .where(col("year") == 2024)
        .columns("id", "amount")
        ._to_spec("orders")
    )
    assert spec.columns == ("id", "amount")
    assert len(spec.predicates) == 1


# ---------------------------------------------------------------------------
# FromFile.columns() — column projection on file source
# ---------------------------------------------------------------------------


def test_from_file_columns_spec_stored(tmp_path: Path) -> None:
    spec = (
        FromFile(str(tmp_path / "data.parquet"), format=Format.PARQUET)
        .columns("order_id", "amount")
        ._to_spec("data")
    )
    assert spec.columns == ("order_id", "amount")


def test_reader_file_columns_projects_parquet(tmp_path: Path) -> None:
    data = pl.DataFrame({"id": [1, 2], "name": ["a", "b"], "value": [10, 20]})
    path = tmp_path / "data.parquet"
    data.write_parquet(str(path))

    spec = SourceSpec(
        alias="data",
        kind=SourceKind.FILE,
        format=Format.PARQUET,
        path=str(path),
        columns=("id", "value"),
    )
    result = _reader(tmp_path).read(spec, None).collect()

    assert set(result.columns) == {"id", "value"}
    assert "name" not in result.columns
