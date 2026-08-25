"""Integration tests for PolarsTargetWriter matched-only UPDATE/MERGE.

Requires polars and deltalake.  The module is skipped automatically when
either is absent (see conftest.py).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from deltalake import DeltaTable, write_deltalake

from loom.etl.backends.polars import PolarsTargetWriter
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target import SchemaMode
from loom.etl.declarative.target._table import UpdateSpec
from loom.etl.schema._schema import SchemaNotFoundError
from loom.etl.storage._config import MissingTablePolicy

from .conftest import table_path

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _update_spec(
    ref: str,
    keys: tuple[str, ...],
    partition_cols: tuple[str, ...] = (),
    exclude: tuple[str, ...] = (),
    include: tuple[str, ...] = (),
    schema_mode: SchemaMode = SchemaMode.STRICT,
) -> UpdateSpec:
    return UpdateSpec(
        table_ref=TableRef(ref),
        keys=keys,
        partition_cols=partition_cols,
        exclude=exclude,
        include=include,
        schema_mode=schema_mode,
    )


def _read_table(root: Path, ref: str) -> pl.DataFrame:
    path = table_path(root, TableRef(ref))
    return pl.scan_delta(str(path)).collect()


def _seed_table(root: Path, ref: str, data: pl.DataFrame) -> None:
    path = table_path(root, TableRef(ref))
    path.mkdir(parents=True, exist_ok=True)
    write_deltalake(str(path), data, mode="overwrite")


def _last_merge_metrics(root: Path, ref: str) -> dict[str, int]:
    dt = DeltaTable(str(table_path(root, TableRef(ref))))
    history = dt.history(1)
    return {k: int(v) for k, v in (history[0].get("operationMetrics") or {}).items()}


def _writer(root: Path) -> PolarsTargetWriter:
    return PolarsTargetWriter(str(root), missing_table_policy=MissingTablePolicy.SCHEMA_MODE)


# ---------------------------------------------------------------------------
# Matched rows are updated; only include= columns change
# ---------------------------------------------------------------------------


def test_update_matched_rows_include_only(tmp_path: Path) -> None:
    initial = pl.DataFrame(
        {
            "id": [1, 2],
            "status": ["pending", "pending"],
            "notes": ["a", "b"],
        }
    )
    _seed_table(tmp_path, "test.orders", initial)
    writer = _writer(tmp_path)

    batch = pl.LazyFrame(
        {
            "id": [1, 2],
            "status": ["done", "done"],
            "notes": ["should not change", "should not change"],
        }
    )
    spec = _update_spec("test.orders", keys=("id",), include=("status",))
    writer.write(batch, spec, None)

    result = _read_table(tmp_path, "test.orders").sort("id")
    assert result["status"].to_list() == ["done", "done"]
    assert result["notes"].to_list() == ["a", "b"]


# ---------------------------------------------------------------------------
# Source rows without a match are ignored — nothing is ever inserted
# ---------------------------------------------------------------------------


def test_update_ignores_unmatched_source_rows(tmp_path: Path) -> None:
    initial = pl.DataFrame({"id": [1, 2], "status": ["pending", "pending"]})
    _seed_table(tmp_path, "test.orders", initial)
    writer = _writer(tmp_path)

    # id=3 has no match in the target: with upsert it would be inserted
    batch = pl.LazyFrame({"id": [2, 3], "status": ["done", "new"]})
    spec = _update_spec("test.orders", keys=("id",))
    writer.write(batch, spec, None)

    result = _read_table(tmp_path, "test.orders").sort("id")
    assert result["id"].to_list() == [1, 2]
    assert result["status"].to_list() == ["pending", "done"]
    metrics = _last_merge_metrics(tmp_path, "test.orders")
    assert metrics["num_target_rows_inserted"] == 0
    assert metrics["num_target_rows_updated"] == 1


# ---------------------------------------------------------------------------
# Partition pre-filter: matched rows in touched partitions, no inserts
# ---------------------------------------------------------------------------


def test_update_multi_partition_never_inserts(tmp_path: Path) -> None:
    initial = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "year": [2023, 2023, 2024],
            "value": [10, 20, 30],
        }
    )
    _seed_table(tmp_path, "test.events", initial)
    writer = _writer(tmp_path)

    # Batch touches year=2023 (update id=1) and year=2025 (no match → ignored)
    batch = pl.LazyFrame(
        {
            "id": [1, 4],
            "year": [2023, 2025],
            "value": [99, 40],
        }
    )
    spec = _update_spec("test.events", keys=("id",), partition_cols=("year",))
    writer.write(batch, spec, None)

    result = _read_table(tmp_path, "test.events").sort("id")
    assert result["id"].to_list() == [1, 2, 3]
    assert result["value"].to_list() == [99, 20, 30]
    assert _last_merge_metrics(tmp_path, "test.events")["num_target_rows_inserted"] == 0


# ---------------------------------------------------------------------------
# Missing table: update never creates — there is nothing to update
# ---------------------------------------------------------------------------


def test_update_missing_table_raises(tmp_path: Path) -> None:
    writer = PolarsTargetWriter(str(tmp_path), missing_table_policy=MissingTablePolicy.CREATE)
    frame = pl.LazyFrame({"id": [1], "name": ["alice"]})
    spec = _update_spec("test.orders", keys=("id",))

    with pytest.raises(SchemaNotFoundError, match="never inserts"):
        writer.write(frame, spec, None)
