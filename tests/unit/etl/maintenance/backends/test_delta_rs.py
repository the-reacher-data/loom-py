"""Integration tests for DeltaRsMaintainer against real local Delta tables."""

from __future__ import annotations

import polars as pl
import pytest
from deltalake import write_deltalake
from deltalake.exceptions import DeltaError

from loom.etl.maintenance._ops import CompactSpec, VacuumSpec, ZOrderSpec
from loom.etl.maintenance.backends._delta_rs import DeltaRsMaintainer
from loom.etl.storage._locator import TableLocation


@pytest.fixture()
def delta_table_uri(tmp_path: object) -> str:
    """Create a small Delta table and return its URI."""
    uri = str(tmp_path) + "/test_table"
    df = pl.DataFrame({"id": [1, 2, 3], "date": ["2024-01-01", "2024-01-02", "2024-01-03"]})
    write_deltalake(uri, df.to_arrow(), mode="overwrite")
    return uri


@pytest.fixture()
def delta_location(delta_table_uri: str) -> TableLocation:
    return TableLocation(uri=delta_table_uri)


class TestDeltaRsMaintainerVacuum:
    def test_vacuum_dry_run_returns_file_list(
        self, delta_table_uri: str, delta_location: TableLocation
    ) -> None:
        maintainer = DeltaRsMaintainer()
        spec = VacuumSpec(dry_run=True)
        result = maintainer.vacuum(delta_table_uri, spec, delta_location)
        assert isinstance(result.files_deleted, list)

    def test_vacuum_dry_run_true_does_not_delete(
        self, delta_table_uri: str, delta_location: TableLocation
    ) -> None:
        import os

        maintainer = DeltaRsMaintainer()
        spec = VacuumSpec(dry_run=True)
        before = set(os.listdir(delta_table_uri))
        maintainer.vacuum(delta_table_uri, spec, delta_location)
        after = set(os.listdir(delta_table_uri))
        assert before == after


class TestDeltaRsMaintainerCompact:
    def test_compact_returns_optimize_result(
        self, delta_table_uri: str, delta_location: TableLocation
    ) -> None:
        maintainer = DeltaRsMaintainer()
        spec = CompactSpec()
        result = maintainer.compact(delta_table_uri, spec, delta_location)
        assert result.num_files_added >= 0
        assert result.num_files_removed >= 0


class TestDeltaRsMaintainerZOrder:
    def test_z_order_returns_optimize_result(
        self, delta_table_uri: str, delta_location: TableLocation
    ) -> None:
        maintainer = DeltaRsMaintainer()
        spec = ZOrderSpec(columns=["id"])
        result = maintainer.z_order(delta_table_uri, spec, delta_location)
        assert result.num_files_added >= 0
        assert result.num_files_removed >= 0

    def test_z_order_nonexistent_column_raises(
        self, delta_table_uri: str, delta_location: TableLocation
    ) -> None:
        maintainer = DeltaRsMaintainer()
        spec = ZOrderSpec(columns=["nonexistent_col"])
        with pytest.raises(DeltaError):
            maintainer.z_order(delta_table_uri, spec, delta_location)
