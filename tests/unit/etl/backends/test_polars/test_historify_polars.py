"""Polars SCD Type 2 historify — backend-specific tests.

Behavioral scenarios live in
:class:`~tests.unit.etl.backends._historify_contract.HistorifyContractTests`.
This module provides the Polars fixtures and backend-specific helper tests.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("polars")
pytest.importorskip("deltalake")

import polars as pl  # noqa: E402

from loom.etl.backends.polars._historify import PolarsHistorifyBackend  # noqa: E402
from loom.etl.backends.polars._writer import PolarsTargetWriter  # noqa: E402
from loom.etl.declarative.target._history import (  # noqa: E402
    HistorifyDateCollisionError,
    HistorifyKeyConflictError,
    HistorifySpec,
)
from loom.etl.storage._config import MissingTablePolicy  # noqa: E402
from tests.unit.etl.backends._historify_contract import (  # noqa: E402
    HistorifyContractTests,
    _log_spec,
    _snapshot_spec,
)


@dataclass
class _Params:
    run_date: date


# ---------------------------------------------------------------------------
# Concrete contract implementation — Polars backend
# ---------------------------------------------------------------------------


class TestHistorifyPolars(HistorifyContractTests):
    """Run all shared behavioral scenarios against PolarsTargetWriter."""

    @pytest.fixture
    def root(self, tmp_path: Path) -> Path:
        return tmp_path

    @pytest.fixture
    def writer(self, tmp_path: Path) -> PolarsTargetWriter:
        return PolarsTargetWriter(str(tmp_path), missing_table_policy=MissingTablePolicy.CREATE)

    @pytest.fixture
    def make_frame(self) -> Callable[[list[dict]], pl.LazyFrame]:
        return lambda rows: pl.from_dicts(rows).lazy()

    @pytest.fixture
    def read_table(self) -> Callable[[str], list[dict[str, Any]]]:
        return lambda uri: pl.scan_delta(uri).collect().to_dicts()


# ---------------------------------------------------------------------------
# Polars-specific helper tests (backend-specific operations)
# ---------------------------------------------------------------------------


def _stamp_new_rows(frame: pl.DataFrame, spec: HistorifySpec, eff_date: Any) -> pl.DataFrame:
    ops = PolarsHistorifyBackend()
    dtype = ops.history_dtype(spec)
    frame = ops.stamp_col(frame, spec.valid_from, eff_date, dtype)
    return ops.stamp_col(frame, spec.valid_to, None, dtype)


class TestAssertUniqueEntityState:
    def test_unique_frame_passes(self) -> None:
        frame = pl.DataFrame({"player_id": [1, 2], "team_id": ["RM", "BCA"]})
        PolarsHistorifyBackend().assert_unique_keys(frame, ["player_id", "team_id"])

    def test_duplicate_raises(self) -> None:
        frame = pl.DataFrame({"player_id": [1, 1], "team_id": ["RM", "RM"]})
        with pytest.raises(HistorifyKeyConflictError):
            PolarsHistorifyBackend().assert_unique_keys(frame, ["player_id", "team_id"])


class TestAssertNoDateCollisions:
    def test_unique_events_pass(self) -> None:
        frame = pl.DataFrame(
            {
                "subscription_id": [1, 1],
                "plan": ["a", "b"],
                "event_date": [date(2024, 1, 1), date(2024, 6, 1)],
            }
        )
        PolarsHistorifyBackend().assert_no_date_collisions(
            frame, ["subscription_id", "plan"], "event_date", _log_spec()
        )

    def test_same_date_same_key_raises(self) -> None:
        frame = pl.DataFrame(
            {
                "subscription_id": [1, 1],
                "plan": ["pro", "pro"],
                "event_date": [date(2024, 1, 1), date(2024, 1, 1)],
            }
        )
        with pytest.raises(HistorifyDateCollisionError):
            PolarsHistorifyBackend().assert_no_date_collisions(
                frame, ["subscription_id", "plan"], "event_date", _log_spec()
            )


class TestStampNewRows:
    def test_adds_history_columns(self) -> None:
        frame = pl.DataFrame({"player_id": [1], "team_id": ["RM"]})
        result = _stamp_new_rows(frame, _snapshot_spec(), date(2024, 1, 1))
        assert result["valid_from"].to_list() == [date(2024, 1, 1)]
        assert result["valid_to"].is_null().all()


class TestIdempotencyStrip:
    def test_strips_rows_from_same_eff_date(self) -> None:
        spec = _snapshot_spec()
        existing = pl.DataFrame(
            {
                "player_id": [1],
                "team_id": ["RM"],
                "valid_from": [date(2024, 6, 1)],
                "valid_to": [None],
            }
        ).with_columns(pl.col("valid_from").cast(pl.Date), pl.col("valid_to").cast(pl.Date))
        result = PolarsHistorifyBackend().rollback_same_day_run(
            existing, spec, date(2024, 6, 1), ["player_id", "team_id"]
        )
        assert len(result) == 0

    def test_reopens_row_closed_by_previous_run(self) -> None:
        spec = _snapshot_spec()
        existing = pl.DataFrame(
            {
                "player_id": [1],
                "team_id": ["RM"],
                "valid_from": [date(2024, 1, 1)],
                "valid_to": [date(2024, 5, 31)],
            }
        ).with_columns(pl.col("valid_from").cast(pl.Date), pl.col("valid_to").cast(pl.Date))
        result = PolarsHistorifyBackend().rollback_same_day_run(
            existing, spec, date(2024, 6, 1), ["player_id", "team_id"]
        )
        assert result["valid_to"].is_null().all()

    def test_preserves_rows_closed_before_run(self) -> None:
        spec = _snapshot_spec()
        existing = pl.DataFrame(
            {
                "player_id": [1, 1],
                "team_id": ["OLD", "RM"],
                "valid_from": [date(2024, 1, 1), date(2024, 4, 1)],
                "valid_to": [date(2024, 3, 31), None],
            }
        ).with_columns(pl.col("valid_from").cast(pl.Date), pl.col("valid_to").cast(pl.Date))
        result = PolarsHistorifyBackend().rollback_same_day_run(
            existing, spec, date(2024, 6, 1), ["player_id", "team_id"]
        )
        old = result.filter(pl.col("team_id") == "OLD")
        assert old["valid_to"].to_list() == [date(2024, 3, 31)]
