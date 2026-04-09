"""Integration tests for PolarsDeltaWriter UPSERT/MERGE.

Requires polars and deltalake.  The module is skipped automatically when
either is absent (see conftest.py).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
from deltalake import write_deltalake

from loom.etl.backends.polars import PolarsDeltaWriter
from loom.etl.io.target import SchemaMode
from loom.etl.io.target._table import UpsertSpec
from loom.etl.schema._table import TableRef
from loom.etl.storage._config import MissingTablePolicy

from .conftest import table_path

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _upsert_spec(
    ref: str,
    keys: tuple[str, ...],
    partition_cols: tuple[str, ...] = (),
    exclude: tuple[str, ...] = (),
    include: tuple[str, ...] = (),
    schema_mode: SchemaMode = SchemaMode.STRICT,
) -> UpsertSpec:
    return UpsertSpec(
        table_ref=TableRef(ref),
        upsert_keys=keys,
        partition_cols=partition_cols,
        upsert_exclude=exclude,
        upsert_include=include,
        schema_mode=schema_mode,
    )


def _read_table(root: Path, ref: str) -> pl.DataFrame:
    path = table_path(root, TableRef(ref))
    return pl.scan_delta(str(path)).collect()


def _seed_table(root: Path, ref: str, data: pl.DataFrame) -> None:
    path = table_path(root, TableRef(ref))
    path.mkdir(parents=True, exist_ok=True)
    write_deltalake(str(path), data, mode="overwrite")


def _writer(
    root: Path,
    *,
    missing_table_policy: MissingTablePolicy = MissingTablePolicy.SCHEMA_MODE,
) -> PolarsDeltaWriter:
    return PolarsDeltaWriter(str(root), missing_table_policy=missing_table_policy)


# ---------------------------------------------------------------------------
# First run: table does not exist → overwrite creates it
# ---------------------------------------------------------------------------


def test_upsert_first_run_creates_table(tmp_path: Path) -> None:
    writer = _writer(tmp_path, missing_table_policy=MissingTablePolicy.CREATE)
    frame = pl.LazyFrame({"id": [1, 2], "name": ["alice", "bob"]})
    spec = _upsert_spec("test.orders", keys=("id",))

    writer.write(frame, spec, None)

    result = _read_table(tmp_path, "test.orders")
    assert sorted(result["id"].to_list()) == [1, 2]


# ---------------------------------------------------------------------------
# Subsequent run: existing rows updated, new rows inserted
# ---------------------------------------------------------------------------


def test_upsert_updates_matched_rows(tmp_path: Path) -> None:
    initial = pl.DataFrame({"id": [1, 2], "status": ["pending", "pending"]})
    _seed_table(tmp_path, "test.orders", initial)
    writer = _writer(tmp_path)

    batch = pl.LazyFrame({"id": [2, 3], "status": ["done", "new"]})
    spec = _upsert_spec("test.orders", keys=("id",))
    writer.write(batch, spec, None)

    result = _read_table(tmp_path, "test.orders").sort("id")
    assert result["id"].to_list() == [1, 2, 3]
    assert result["status"].to_list() == ["pending", "done", "new"]


# ---------------------------------------------------------------------------
# exclude= keeps specified columns unchanged on match
# ---------------------------------------------------------------------------


def test_upsert_exclude_preserves_column(tmp_path: Path) -> None:
    initial = pl.DataFrame(
        {
            "id": [1],
            "status": ["pending"],
            "created_at": ["2024-01-01"],
        }
    )
    _seed_table(tmp_path, "test.orders", initial)
    writer = _writer(tmp_path)

    batch = pl.LazyFrame(
        {
            "id": [1],
            "status": ["done"],
            "created_at": ["9999-99-99"],
        }
    )
    spec = _upsert_spec("test.orders", keys=("id",), exclude=("created_at",))
    writer.write(batch, spec, None)

    result = _read_table(tmp_path, "test.orders")
    assert result["status"].to_list() == ["done"]
    assert result["created_at"].to_list() == ["2024-01-01"]


# ---------------------------------------------------------------------------
# include= updates only the listed columns on match
# ---------------------------------------------------------------------------


def test_upsert_include_updates_only_listed_columns(tmp_path: Path) -> None:
    initial = pl.DataFrame(
        {
            "id": [1],
            "status": ["pending"],
            "notes": ["original"],
        }
    )
    _seed_table(tmp_path, "test.orders", initial)
    writer = _writer(tmp_path)

    batch = pl.LazyFrame(
        {
            "id": [1],
            "status": ["done"],
            "notes": ["should not change"],
        }
    )
    spec = _upsert_spec("test.orders", keys=("id",), include=("status",))
    writer.write(batch, spec, None)

    result = _read_table(tmp_path, "test.orders")
    assert result["status"].to_list() == ["done"]
    assert result["notes"].to_list() == ["original"]


# ---------------------------------------------------------------------------
# Multi-partition batch: only touched partitions are merged
# ---------------------------------------------------------------------------


def test_upsert_multi_partition_merges_correctly(tmp_path: Path) -> None:
    initial = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "year": [2023, 2023, 2024],
            "value": [10, 20, 30],
        }
    )
    _seed_table(tmp_path, "test.events", initial)
    writer = _writer(tmp_path)

    # Batch touches year=2023 (update id=1) and year=2025 (insert id=4)
    batch = pl.LazyFrame(
        {
            "id": [1, 4],
            "year": [2023, 2025],
            "value": [99, 40],
        }
    )
    spec = _upsert_spec("test.events", keys=("id",), partition_cols=("year",))
    writer.write(batch, spec, None)

    result = _read_table(tmp_path, "test.events").sort("id")
    assert result["id"].to_list() == [1, 2, 3, 4]
    assert result["value"].to_list() == [99, 20, 30, 40]


# ---------------------------------------------------------------------------
# Upsert inserts new rows without affecting existing ones
# ---------------------------------------------------------------------------


def test_upsert_inserts_new_rows_without_affecting_existing(tmp_path: Path) -> None:
    initial = pl.DataFrame({"id": [1], "name": ["alice"]})
    _seed_table(tmp_path, "test.users", initial)
    writer = _writer(tmp_path)

    batch = pl.LazyFrame({"id": [2], "name": ["bob"]})
    spec = _upsert_spec("test.users", keys=("id",))
    writer.write(batch, spec, None)

    result = _read_table(tmp_path, "test.users")
    assert sorted(result["id"].to_list()) == [1, 2]
    assert set(result.columns) == {"id", "name"}
