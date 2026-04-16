"""Unit tests for Spark SCD Type 2 historify via SparkTargetWriter."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from loom.etl.backends._historify._common import resolve_track_cols  # noqa: E402
from loom.etl.backends.spark._ops import SparkFrameOps  # noqa: E402
from loom.etl.backends.spark._writer import SparkTargetWriter  # noqa: E402
from loom.etl.declarative.expr._params import params as p  # noqa: E402
from loom.etl.declarative.expr._refs import TableRef  # noqa: E402
from loom.etl.declarative.target._history import (  # noqa: E402
    DeletePolicy,
    HistorifyDateCollisionError,
    HistorifyInputMode,
    HistorifyKeyConflictError,
    HistorifySpec,
    HistorifyTemporalConflictError,
)
from loom.etl.declarative.target._schema_mode import SchemaMode  # noqa: E402
from loom.etl.storage._config import MissingTablePolicy  # noqa: E402
from loom.etl.storage._locator import TableLocation  # noqa: E402
from loom.etl.storage.routing import FixedPathRouteResolver  # noqa: E402


@dataclass
class _Params:
    run_date: date


def _snapshot_spec(
    *,
    keys: tuple[str, ...] = ("player_id",),
    track: tuple[str, ...] | None = ("team_id",),
    delete_policy: DeletePolicy = DeletePolicy.CLOSE,
    partition_scope: tuple[str, ...] | None = None,
    allow_temporal_rerun: bool = False,
) -> HistorifySpec:
    return HistorifySpec(
        table_ref=TableRef("dim_players"),
        keys=keys,
        effective_date=p.run_date,
        mode=HistorifyInputMode.SNAPSHOT,
        track=track,
        delete_policy=delete_policy,
        partition_scope=partition_scope,
        schema_mode=SchemaMode.STRICT,
        allow_temporal_rerun=allow_temporal_rerun,
    )


def _log_spec(
    *,
    keys: tuple[str, ...] = ("subscription_id",),
    track: tuple[str, ...] | None = ("plan",),
) -> HistorifySpec:
    return HistorifySpec(
        table_ref=TableRef("dim_subs"),
        keys=keys,
        effective_date="event_date",
        mode=HistorifyInputMode.LOG,
        track=track,
        schema_mode=SchemaMode.STRICT,
    )


def _writer(spark: SparkSession, uri: str) -> SparkTargetWriter:
    location = TableLocation(uri=uri)
    return SparkTargetWriter(
        spark,
        route_resolver=FixedPathRouteResolver(location),
        missing_table_policy=MissingTablePolicy.CREATE,
    )


def _uri(spark_root: Path, name: str = "dim_players") -> str:
    return str(spark_root / name)


def _read(spark: SparkSession, uri: str) -> list[dict[str, Any]]:
    return [row.asDict() for row in spark.read.format("delta").load(uri).collect()]


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------


class TestBootstrap:
    def test_creates_table(self, spark: SparkSession, spark_root: Path) -> None:
        uri = _uri(spark_root)
        frame = spark.createDataFrame(
            [{"player_id": 1, "team_id": "RM"}, {"player_id": 2, "team_id": "BCA"}]
        )
        _writer(spark, uri).write(frame, _snapshot_spec(), _Params(run_date=date(2024, 1, 1)))
        assert len(_read(spark, uri)) == 2

    def test_valid_from_equals_eff_date(self, spark: SparkSession, spark_root: Path) -> None:
        uri = _uri(spark_root)
        frame = spark.createDataFrame([{"player_id": 1, "team_id": "RM"}])
        _writer(spark, uri).write(frame, _snapshot_spec(), _Params(run_date=date(2024, 1, 1)))
        rows = _read(spark, uri)
        assert rows[0]["valid_from"] == date(2024, 1, 1)

    def test_valid_to_is_null(self, spark: SparkSession, spark_root: Path) -> None:
        uri = _uri(spark_root)
        frame = spark.createDataFrame(
            [{"player_id": 1, "team_id": "RM"}, {"player_id": 2, "team_id": "BCA"}]
        )
        _writer(spark, uri).write(frame, _snapshot_spec(), _Params(run_date=date(2024, 1, 1)))
        rows = _read(spark, uri)
        assert all(r["valid_to"] is None for r in rows)


# ---------------------------------------------------------------------------
# No-op (no changes)
# ---------------------------------------------------------------------------


class TestNoOp:
    def test_no_new_rows_when_unchanged(self, spark: SparkSession, spark_root: Path) -> None:
        uri = _uri(spark_root)
        frame = spark.createDataFrame([{"player_id": 1, "team_id": "RM"}])
        w = _writer(spark, uri)
        w.write(frame, _snapshot_spec(), _Params(run_date=date(2024, 1, 1)))
        w.write(frame, _snapshot_spec(), _Params(run_date=date(2024, 1, 2)))
        rows = _read(spark, uri)
        assert len(rows) == 1
        assert rows[0]["valid_to"] is None

    def test_valid_from_preserved_on_no_change(self, spark: SparkSession, spark_root: Path) -> None:
        uri = _uri(spark_root)
        frame = spark.createDataFrame([{"player_id": 1, "team_id": "RM"}])
        w = _writer(spark, uri)
        w.write(frame, _snapshot_spec(), _Params(run_date=date(2024, 1, 1)))
        w.write(frame, _snapshot_spec(), _Params(run_date=date(2024, 1, 2)))
        rows = _read(spark, uri)
        assert rows[0]["valid_from"] == date(2024, 1, 1)


# ---------------------------------------------------------------------------
# New entity
# ---------------------------------------------------------------------------


class TestNewEntity:
    def test_new_player_inserted(self, spark: SparkSession, spark_root: Path) -> None:
        uri = _uri(spark_root)
        w = _writer(spark, uri)
        w.write(
            spark.createDataFrame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        w.write(
            spark.createDataFrame(
                [{"player_id": 1, "team_id": "RM"}, {"player_id": 2, "team_id": "BCA"}]
            ),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 5)),
        )
        assert len(_read(spark, uri)) == 2

    def test_new_player_valid_from_is_eff_date(self, spark: SparkSession, spark_root: Path) -> None:
        uri = _uri(spark_root)
        w = _writer(spark, uri)
        w.write(
            spark.createDataFrame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        w.write(
            spark.createDataFrame(
                [{"player_id": 1, "team_id": "RM"}, {"player_id": 2, "team_id": "BCA"}]
            ),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 5)),
        )
        rows = _read(spark, uri)
        p2 = [r for r in rows if r["player_id"] == 2]
        assert p2[0]["valid_from"] == date(2024, 1, 5)
        assert p2[0]["valid_to"] is None


# ---------------------------------------------------------------------------
# Tracked column change
# ---------------------------------------------------------------------------


class TestTrackedChange:
    def test_two_rows_after_team_change(self, spark: SparkSession, spark_root: Path) -> None:
        uri = _uri(spark_root)
        w = _writer(spark, uri)
        w.write(
            spark.createDataFrame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        w.write(
            spark.createDataFrame([{"player_id": 1, "team_id": "BCA"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 6, 1)),
        )
        assert len(_read(spark, uri)) == 2

    def test_old_row_valid_to_is_prev_day(self, spark: SparkSession, spark_root: Path) -> None:
        uri = _uri(spark_root)
        w = _writer(spark, uri)
        w.write(
            spark.createDataFrame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        w.write(
            spark.createDataFrame([{"player_id": 1, "team_id": "BCA"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = _read(spark, uri)
        old = [r for r in rows if r["team_id"] == "RM"]
        assert old[0]["valid_to"] == date(2024, 5, 31)

    def test_new_row_is_open(self, spark: SparkSession, spark_root: Path) -> None:
        uri = _uri(spark_root)
        w = _writer(spark, uri)
        w.write(
            spark.createDataFrame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        w.write(
            spark.createDataFrame([{"player_id": 1, "team_id": "BCA"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = _read(spark, uri)
        new = [r for r in rows if r["team_id"] == "BCA"]
        assert new[0]["valid_from"] == date(2024, 6, 1)
        assert new[0]["valid_to"] is None


# ---------------------------------------------------------------------------
# Delete policies
# ---------------------------------------------------------------------------


class TestDeletePolicies:
    def test_close_sets_valid_to(self, spark: SparkSession, spark_root: Path) -> None:
        uri = _uri(spark_root)
        w = _writer(spark, uri)
        w.write(
            spark.createDataFrame(
                [{"player_id": 1, "team_id": "RM"}, {"player_id": 2, "team_id": "BCA"}]
            ),
            _snapshot_spec(delete_policy=DeletePolicy.CLOSE),
            _Params(run_date=date(2024, 1, 1)),
        )
        w.write(
            spark.createDataFrame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(delete_policy=DeletePolicy.CLOSE),
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = _read(spark, uri)
        p2 = [r for r in rows if r["player_id"] == 2]
        assert p2[0]["valid_to"] == date(2024, 5, 31)

    def test_ignore_leaves_vector_open(self, spark: SparkSession, spark_root: Path) -> None:
        uri = _uri(spark_root)
        w = _writer(spark, uri)
        w.write(
            spark.createDataFrame(
                [{"player_id": 1, "team_id": "RM"}, {"player_id": 2, "team_id": "BCA"}]
            ),
            _snapshot_spec(delete_policy=DeletePolicy.IGNORE),
            _Params(run_date=date(2024, 1, 1)),
        )
        w.write(
            spark.createDataFrame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(delete_policy=DeletePolicy.IGNORE),
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = _read(spark, uri)
        p2 = [r for r in rows if r["player_id"] == 2]
        assert p2[0]["valid_to"] is None

    def test_soft_delete_sets_deleted_at(self, spark: SparkSession, spark_root: Path) -> None:
        uri = _uri(spark_root)
        w = _writer(spark, uri)
        w.write(
            spark.createDataFrame(
                [{"player_id": 1, "team_id": "RM"}, {"player_id": 2, "team_id": "BCA"}]
            ),
            _snapshot_spec(delete_policy=DeletePolicy.SOFT_DELETE),
            _Params(run_date=date(2024, 1, 1)),
        )
        w.write(
            spark.createDataFrame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(delete_policy=DeletePolicy.SOFT_DELETE),
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = _read(spark, uri)
        p2 = [r for r in rows if r["player_id"] == 2]
        assert p2[0]["deleted_at"] == date(2024, 6, 1)
        assert p2[0]["valid_to"] == date(2024, 5, 31)


# ---------------------------------------------------------------------------
# Loan case — multiple simultaneous open vectors
# ---------------------------------------------------------------------------


class TestLoanCase:
    def test_two_vectors_open_simultaneously(self, spark: SparkSession, spark_root: Path) -> None:
        uri = _uri(spark_root)
        spec = _snapshot_spec(keys=("player_id",), track=("team_id", "role"))
        frame = spark.createDataFrame(
            [
                {"player_id": 1, "team_id": "RM", "role": "owner"},
                {"player_id": 1, "team_id": "GET", "role": "loan"},
            ]
        )
        _writer(spark, uri).write(frame, spec, _Params(run_date=date(2024, 1, 1)))
        rows = _read(spark, uri)
        assert len(rows) == 2
        assert all(r["valid_to"] is None for r in rows)

    def test_loan_end_closes_only_loan_vector(self, spark: SparkSession, spark_root: Path) -> None:
        uri = _uri(spark_root)
        spec = _snapshot_spec(keys=("player_id",), track=("team_id", "role"))
        w = _writer(spark, uri)
        w.write(
            spark.createDataFrame(
                [
                    {"player_id": 1, "team_id": "RM", "role": "owner"},
                    {"player_id": 1, "team_id": "GET", "role": "loan"},
                ]
            ),
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        w.write(
            spark.createDataFrame([{"player_id": 1, "team_id": "RM", "role": "owner"}]),
            spec,
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = _read(spark, uri)
        assert len(rows) == 2
        loan = [r for r in rows if r["role"] == "loan"]
        assert loan[0]["valid_to"] == date(2024, 5, 31)
        owner = [r for r in rows if r["role"] == "owner"]
        assert owner[0]["valid_to"] is None


# ---------------------------------------------------------------------------
# Passive columns
# ---------------------------------------------------------------------------


class TestPassiveColumns:
    def test_passive_change_does_not_trigger_new_row(
        self, spark: SparkSession, spark_root: Path
    ) -> None:
        uri = _uri(spark_root)
        spec = _snapshot_spec(keys=("player_id",), track=("team_id",))
        w = _writer(spark, uri)
        w.write(
            spark.createDataFrame([{"player_id": 1, "team_id": "RM", "salary": 100}]),
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        w.write(
            spark.createDataFrame([{"player_id": 1, "team_id": "RM", "salary": 150}]),
            spec,
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = _read(spark, uri)
        assert len(rows) == 1
        assert rows[0]["salary"] == 100


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


class TestIdempotency:
    def test_same_day_rerun_stable_row_count(self, spark: SparkSession, spark_root: Path) -> None:
        uri = _uri(spark_root)
        w = _writer(spark, uri)
        w.write(
            spark.createDataFrame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        frame2 = spark.createDataFrame([{"player_id": 1, "team_id": "BCA"}])
        w.write(frame2, _snapshot_spec(), _Params(run_date=date(2024, 6, 1)))
        w.write(frame2, _snapshot_spec(), _Params(run_date=date(2024, 6, 1)))
        assert len(_read(spark, uri)) == 2

    def test_same_day_rerun_stable_valid_to(self, spark: SparkSession, spark_root: Path) -> None:
        uri = _uri(spark_root)
        w = _writer(spark, uri)
        w.write(
            spark.createDataFrame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        frame2 = spark.createDataFrame([{"player_id": 1, "team_id": "BCA"}])
        for _ in range(3):
            w.write(frame2, _snapshot_spec(), _Params(run_date=date(2024, 6, 1)))
        rows = _read(spark, uri)
        old = [r for r in rows if r["team_id"] == "RM"]
        assert old[0]["valid_to"] == date(2024, 5, 31)


# ---------------------------------------------------------------------------
# Temporal conflict guard
# ---------------------------------------------------------------------------


class TestTemporalGuard:
    def test_backfill_raises_when_disabled(self, spark: SparkSession, spark_root: Path) -> None:
        uri = _uri(spark_root)
        w = _writer(spark, uri)
        w.write(
            spark.createDataFrame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 6, 1)),
        )
        with pytest.raises(HistorifyTemporalConflictError):
            w.write(
                spark.createDataFrame([{"player_id": 1, "team_id": "RM"}]),
                _snapshot_spec(allow_temporal_rerun=False),
                _Params(run_date=date(2024, 1, 1)),
            )

    def test_backfill_allowed_when_flag_set(self, spark: SparkSession, spark_root: Path) -> None:
        uri = _uri(spark_root)
        w = _writer(spark, uri)
        w.write(
            spark.createDataFrame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 6, 1)),
        )
        w.write(
            spark.createDataFrame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(allow_temporal_rerun=True),
            _Params(run_date=date(2024, 1, 1)),
        )


# ---------------------------------------------------------------------------
# LOG mode
# ---------------------------------------------------------------------------


class TestLogBootstrap:
    def test_events_get_valid_from(self, spark: SparkSession, spark_root: Path) -> None:
        uri = _uri(spark_root, "dim_subs")
        frame = spark.createDataFrame(
            [
                {"subscription_id": 1, "plan": "basic", "event_date": date(2024, 1, 1)},
                {"subscription_id": 1, "plan": "pro", "event_date": date(2024, 6, 1)},
            ]
        )
        _writer(spark, uri).write(frame, _log_spec(), None)
        rows = sorted(_read(spark, uri), key=lambda r: r["valid_from"])
        assert [r["valid_from"] for r in rows] == [date(2024, 1, 1), date(2024, 6, 1)]

    def test_first_event_closed_by_second(self, spark: SparkSession, spark_root: Path) -> None:
        uri = _uri(spark_root, "dim_subs")
        frame = spark.createDataFrame(
            [
                {"subscription_id": 1, "plan": "basic", "event_date": date(2024, 1, 1)},
                {"subscription_id": 1, "plan": "pro", "event_date": date(2024, 6, 1)},
            ]
        )
        _writer(spark, uri).write(frame, _log_spec(), None)
        rows = _read(spark, uri)
        basic = [r for r in rows if r["plan"] == "basic"]
        assert basic[0]["valid_to"] == date(2024, 5, 31)

    def test_last_event_is_open(self, spark: SparkSession, spark_root: Path) -> None:
        uri = _uri(spark_root, "dim_subs")
        frame = spark.createDataFrame(
            [
                {"subscription_id": 1, "plan": "basic", "event_date": date(2024, 1, 1)},
                {"subscription_id": 1, "plan": "pro", "event_date": date(2024, 6, 1)},
            ]
        )
        _writer(spark, uri).write(frame, _log_spec(), None)
        rows = _read(spark, uri)
        pro = [r for r in rows if r["plan"] == "pro"]
        assert pro[0]["valid_to"] is None


class TestLogIncremental:
    def test_new_event_closes_previous(self, spark: SparkSession, spark_root: Path) -> None:
        uri = _uri(spark_root, "dim_subs")
        w = _writer(spark, uri)
        w.write(
            spark.createDataFrame(
                [{"subscription_id": 1, "plan": "basic", "event_date": date(2024, 1, 1)}]
            ),
            _log_spec(),
            None,
        )
        w.write(
            spark.createDataFrame(
                [{"subscription_id": 1, "plan": "pro", "event_date": date(2024, 6, 1)}]
            ),
            _log_spec(),
            None,
        )
        rows = _read(spark, uri)
        assert len(rows) == 2
        basic = [r for r in rows if r["plan"] == "basic"]
        assert basic[0]["valid_to"] == date(2024, 5, 31)

    def test_unaffected_entities_preserved(self, spark: SparkSession, spark_root: Path) -> None:
        uri = _uri(spark_root, "dim_subs")
        w = _writer(spark, uri)
        w.write(
            spark.createDataFrame(
                [
                    {"subscription_id": 1, "plan": "basic", "event_date": date(2024, 1, 1)},
                    {"subscription_id": 2, "plan": "pro", "event_date": date(2024, 1, 1)},
                ]
            ),
            _log_spec(),
            None,
        )
        w.write(
            spark.createDataFrame(
                [{"subscription_id": 1, "plan": "enterprise", "event_date": date(2024, 9, 1)}]
            ),
            _log_spec(),
            None,
        )
        rows = _read(spark, uri)
        sub2 = [r for r in rows if r["subscription_id"] == 2]
        assert len(sub2) == 1
        assert sub2[0]["valid_to"] is None


# ---------------------------------------------------------------------------
# Pure helper unit tests
# ---------------------------------------------------------------------------


def _stamp_new_rows(frame: Any, spec: HistorifySpec, eff_date: Any) -> Any:
    """Stamp valid_from / valid_to onto a frame (mirrors first-run bootstrap logic)."""
    ops = SparkFrameOps()
    dtype = ops.history_dtype(spec)
    frame = ops.stamp_col(frame, spec.valid_from, eff_date, dtype)
    return ops.stamp_col(frame, spec.valid_to, None, dtype)


class TestResolveTrackCols:
    def test_explicit_track_returned(self) -> None:
        spec = _snapshot_spec(track=("team_id", "salary"))
        result = resolve_track_cols(spec, ["player_id", "team_id", "salary", "note"])
        assert result == ("team_id", "salary")

    def test_none_track_excludes_keys_and_history(self) -> None:
        spec = _snapshot_spec(keys=("player_id",), track=None)
        result = resolve_track_cols(spec, ["player_id", "team_id", "salary"])
        assert set(result) == {"team_id", "salary"}


class TestAssertUniqueEntityState:
    def test_unique_frame_passes(self, spark: SparkSession) -> None:
        frame = spark.createDataFrame(
            [{"player_id": 1, "team_id": "RM"}, {"player_id": 2, "team_id": "BCA"}]
        )
        SparkFrameOps().assert_unique_keys(frame, ["player_id", "team_id"])

    def test_duplicate_raises(self, spark: SparkSession) -> None:
        frame = spark.createDataFrame(
            [{"player_id": 1, "team_id": "RM"}, {"player_id": 1, "team_id": "RM"}]
        )
        with pytest.raises(HistorifyKeyConflictError):
            SparkFrameOps().assert_unique_keys(frame, ["player_id", "team_id"])


class TestAssertNoDateCollisions:
    def test_unique_events_pass(self, spark: SparkSession) -> None:
        frame = spark.createDataFrame(
            [
                {"subscription_id": 1, "plan": "a", "event_date": date(2024, 1, 1)},
                {"subscription_id": 1, "plan": "b", "event_date": date(2024, 6, 1)},
            ]
        )
        SparkFrameOps().assert_no_date_collisions(
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
            SparkFrameOps().assert_no_date_collisions(
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
        result = SparkFrameOps().rollback_same_day_run(
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
        result = SparkFrameOps().rollback_same_day_run(
            existing, spec, date(2024, 6, 1), ["player_id", "team_id"]
        )
        rows = result.collect()
        assert rows[0]["valid_to"] is None
