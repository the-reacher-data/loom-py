"""Spark SCD Type 2 historify — backend-specific tests.

Behavioral scenarios live in
:class:`~tests.unit.etl.backends._historify_contract.HistorifyContractTests`.
This module provides the Spark fixtures and backend-specific helper tests.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from loom.etl.backends.spark._historify import SparkHistorifyBackend  # noqa: E402
from loom.etl.backends.spark._writer import SparkTargetWriter  # noqa: E402
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

# ---------------------------------------------------------------------------
# Concrete contract implementation — Spark backend
# ---------------------------------------------------------------------------


class TestHistorifySparkTarget(HistorifyContractTests):
    """Run all shared behavioral scenarios against SparkTargetWriter."""

    @pytest.fixture
    def root(self, spark_root: Path) -> Path:
        return spark_root

    @pytest.fixture
    def writer(self, spark: SparkSession, spark_root: Path) -> SparkTargetWriter:
        return SparkTargetWriter(
            spark,
            str(spark_root),
            missing_table_policy=MissingTablePolicy.CREATE,
        )

    @pytest.fixture
    def make_frame(self, spark: SparkSession) -> Callable[[list[dict]], DataFrame]:
        return spark.createDataFrame

    @pytest.fixture
    def read_table(self, spark: SparkSession) -> Callable[[str], list[dict[str, Any]]]:
        return lambda uri: [row.asDict() for row in spark.read.format("delta").load(uri).collect()]


# ---------------------------------------------------------------------------
# Spark-specific helper tests (backend-specific operations)
# ---------------------------------------------------------------------------


def _stamp_new_rows(frame: DataFrame, spec: HistorifySpec, eff_date: Any) -> DataFrame:
    ops = SparkHistorifyBackend()
    dtype = ops.history_dtype(spec)
    frame = ops.stamp_col(frame, spec.valid_from, eff_date, dtype)
    return ops.stamp_col(frame, spec.valid_to, None, dtype)


class TestAssertUniqueEntityState:
    def test_unique_frame_passes(self, spark: SparkSession) -> None:
        frame = spark.createDataFrame(
            [{"player_id": 1, "team_id": "RM"}, {"player_id": 2, "team_id": "BCA"}]
        )
        SparkHistorifyBackend().assert_unique_keys(frame, ["player_id", "team_id"])

    def test_duplicate_raises(self, spark: SparkSession) -> None:
        frame = spark.createDataFrame(
            [{"player_id": 1, "team_id": "RM"}, {"player_id": 1, "team_id": "RM"}]
        )
        with pytest.raises(HistorifyKeyConflictError):
            SparkHistorifyBackend().assert_unique_keys(frame, ["player_id", "team_id"])


class TestAssertNoDateCollisions:
    def test_unique_events_pass(self, spark: SparkSession) -> None:
        frame = spark.createDataFrame(
            [
                {"subscription_id": 1, "plan": "a", "event_date": date(2024, 1, 1)},
                {"subscription_id": 1, "plan": "b", "event_date": date(2024, 6, 1)},
            ]
        )
        SparkHistorifyBackend().assert_no_date_collisions(
            frame, ["subscription_id", "plan"], "event_date", _log_spec()
        )

    def test_same_date_same_key_raises(self, spark: SparkSession) -> None:
        frame = spark.createDataFrame(
            [
                {"subscription_id": 1, "plan": "pro", "event_date": date(2024, 1, 1)},
                {"subscription_id": 1, "plan": "pro", "event_date": date(2024, 1, 1)},
            ]
        )
        with pytest.raises(HistorifyDateCollisionError):
            SparkHistorifyBackend().assert_no_date_collisions(
                frame, ["subscription_id", "plan"], "event_date", _log_spec()
            )


class TestStampNewRows:
    def test_adds_history_columns(self, spark: SparkSession) -> None:
        frame = spark.createDataFrame([{"player_id": 1, "team_id": "RM"}])
        spec = _snapshot_spec()
        result = _stamp_new_rows(frame, spec, date(2024, 1, 1))
        rows = result.collect()
        assert rows[0]["valid_from"] == date(2024, 1, 1)
        assert rows[0]["valid_to"] is None


class TestIdempotencyStrip:
    def test_strips_rows_from_same_eff_date(self, spark: SparkSession) -> None:
        spec = _snapshot_spec()
        schema = T.StructType(
            [
                T.StructField("player_id", T.LongType()),
                T.StructField("team_id", T.StringType()),
                T.StructField("valid_from", T.DateType()),
                T.StructField("valid_to", T.DateType()),
            ]
        )
        existing = spark.createDataFrame(
            [{"player_id": 1, "team_id": "RM", "valid_from": date(2024, 6, 1), "valid_to": None}],
            schema=schema,
        )
        result = SparkHistorifyBackend().rollback_same_day_run(
            existing, spec, date(2024, 6, 1), ["player_id", "team_id"]
        )
        assert result.count() == 0

    def test_reopens_row_closed_by_previous_run(self, spark: SparkSession) -> None:
        spec = _snapshot_spec()
        schema = T.StructType(
            [
                T.StructField("player_id", T.LongType()),
                T.StructField("team_id", T.StringType()),
                T.StructField("valid_from", T.DateType()),
                T.StructField("valid_to", T.DateType()),
            ]
        )
        existing = spark.createDataFrame(
            [
                {
                    "player_id": 1,
                    "team_id": "RM",
                    "valid_from": date(2024, 1, 1),
                    "valid_to": date(2024, 5, 31),
                }
            ],
            schema=schema,
        )
        result = SparkHistorifyBackend().rollback_same_day_run(
            existing, spec, date(2024, 6, 1), ["player_id", "team_id"]
        )
        rows = result.collect()
        assert rows[0]["valid_to"] is None
