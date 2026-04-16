"""Unit tests for PolarsHistorifyEngine — SCD Type 2 with real Delta tables."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pytest

pytest.importorskip("polars")
pytest.importorskip("deltalake")

import polars as pl  # noqa: E402

from loom.etl.backends.polars._historify import (  # noqa: E402
    PolarsHistorifyEngine,
    _assert_no_date_collisions,
    _assert_unique_entity_state,
    _idempotency_strip,
    _resolve_track_cols,
    _stamp_new_rows,
)
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
        table_ref=TableRef("wh.dim_players"),
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
        table_ref=TableRef("wh.dim_subs"),
        keys=keys,
        effective_date="event_date",
        mode=HistorifyInputMode.LOG,
        track=track,
        schema_mode=SchemaMode.STRICT,
    )


def _uri(tmp_path: Path, name: str = "dim_players") -> str:
    path = tmp_path / name
    path.mkdir(parents=True, exist_ok=True)
    return str(path)


def _eng() -> PolarsHistorifyEngine:
    return PolarsHistorifyEngine()


def _read(uri: str) -> pl.DataFrame:
    return pl.scan_delta(uri).collect()


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------


class TestBootstrap:
    def test_creates_table(self, tmp_path: Path) -> None:
        uri = _uri(tmp_path)
        frame = pl.DataFrame({"player_id": [1, 2], "team_id": ["RM", "BCA"]})
        _eng().apply(frame, uri, None, _snapshot_spec(), _Params(run_date=date(2024, 1, 1)))
        assert len(_read(uri)) == 2

    def test_valid_from_equals_eff_date(self, tmp_path: Path) -> None:
        uri = _uri(tmp_path)
        frame = pl.DataFrame({"player_id": [1], "team_id": ["RM"]})
        _eng().apply(frame, uri, None, _snapshot_spec(), _Params(run_date=date(2024, 1, 1)))
        assert _read(uri)["valid_from"].to_list() == [date(2024, 1, 1)]

    def test_valid_to_is_null(self, tmp_path: Path) -> None:
        uri = _uri(tmp_path)
        frame = pl.DataFrame({"player_id": [1, 2], "team_id": ["RM", "BCA"]})
        _eng().apply(frame, uri, None, _snapshot_spec(), _Params(run_date=date(2024, 1, 1)))
        assert _read(uri)["valid_to"].is_null().all()


# ---------------------------------------------------------------------------
# No-op (no changes)
# ---------------------------------------------------------------------------


class TestNoOp:
    def test_no_new_rows_when_unchanged(self, tmp_path: Path) -> None:
        uri = _uri(tmp_path)
        frame = pl.DataFrame({"player_id": [1], "team_id": ["RM"]})
        eng = _eng()
        eng.apply(frame, uri, None, _snapshot_spec(), _Params(run_date=date(2024, 1, 1)))
        eng.apply(frame, uri, None, _snapshot_spec(), _Params(run_date=date(2024, 1, 2)))
        result = _read(uri)
        assert len(result) == 1
        assert result["valid_to"].is_null().all()

    def test_valid_from_preserved_on_no_change(self, tmp_path: Path) -> None:
        uri = _uri(tmp_path)
        frame = pl.DataFrame({"player_id": [1], "team_id": ["RM"]})
        eng = _eng()
        eng.apply(frame, uri, None, _snapshot_spec(), _Params(run_date=date(2024, 1, 1)))
        eng.apply(frame, uri, None, _snapshot_spec(), _Params(run_date=date(2024, 1, 2)))
        assert _read(uri)["valid_from"].to_list() == [date(2024, 1, 1)]


# ---------------------------------------------------------------------------
# New entity
# ---------------------------------------------------------------------------


class TestNewEntity:
    def test_new_player_inserted(self, tmp_path: Path) -> None:
        uri = _uri(tmp_path)
        eng = _eng()
        eng.apply(
            pl.DataFrame({"player_id": [1], "team_id": ["RM"]}),
            uri,
            None,
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        eng.apply(
            pl.DataFrame({"player_id": [1, 2], "team_id": ["RM", "BCA"]}),
            uri,
            None,
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 5)),
        )
        result = _read(uri)
        assert len(result) == 2

    def test_new_player_valid_from_is_eff_date(self, tmp_path: Path) -> None:
        uri = _uri(tmp_path)
        eng = _eng()
        eng.apply(
            pl.DataFrame({"player_id": [1], "team_id": ["RM"]}),
            uri,
            None,
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        eng.apply(
            pl.DataFrame({"player_id": [1, 2], "team_id": ["RM", "BCA"]}),
            uri,
            None,
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 5)),
        )
        p2 = _read(uri).filter(pl.col("player_id") == 2)
        assert p2["valid_from"].to_list() == [date(2024, 1, 5)]
        assert p2["valid_to"].is_null().all()


# ---------------------------------------------------------------------------
# Tracked column change
# ---------------------------------------------------------------------------


class TestTrackedChange:
    def test_two_rows_after_team_change(self, tmp_path: Path) -> None:
        uri = _uri(tmp_path)
        eng = _eng()
        eng.apply(
            pl.DataFrame({"player_id": [1], "team_id": ["RM"]}),
            uri,
            None,
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        eng.apply(
            pl.DataFrame({"player_id": [1], "team_id": ["BCA"]}),
            uri,
            None,
            _snapshot_spec(),
            _Params(run_date=date(2024, 6, 1)),
        )
        assert len(_read(uri)) == 2

    def test_old_row_valid_to_is_prev_day(self, tmp_path: Path) -> None:
        uri = _uri(tmp_path)
        eng = _eng()
        eng.apply(
            pl.DataFrame({"player_id": [1], "team_id": ["RM"]}),
            uri,
            None,
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        eng.apply(
            pl.DataFrame({"player_id": [1], "team_id": ["BCA"]}),
            uri,
            None,
            _snapshot_spec(),
            _Params(run_date=date(2024, 6, 1)),
        )
        old = _read(uri).filter(pl.col("team_id") == "RM")
        assert old["valid_to"].to_list() == [date(2024, 5, 31)]

    def test_new_row_is_open(self, tmp_path: Path) -> None:
        uri = _uri(tmp_path)
        eng = _eng()
        eng.apply(
            pl.DataFrame({"player_id": [1], "team_id": ["RM"]}),
            uri,
            None,
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        eng.apply(
            pl.DataFrame({"player_id": [1], "team_id": ["BCA"]}),
            uri,
            None,
            _snapshot_spec(),
            _Params(run_date=date(2024, 6, 1)),
        )
        new = _read(uri).filter(pl.col("team_id") == "BCA")
        assert new["valid_from"].to_list() == [date(2024, 6, 1)]
        assert new["valid_to"].is_null().all()


# ---------------------------------------------------------------------------
# Delete policies
# ---------------------------------------------------------------------------


class TestDeletePolicies:
    def test_close_sets_valid_to(self, tmp_path: Path) -> None:
        uri = _uri(tmp_path)
        eng = _eng()
        eng.apply(
            pl.DataFrame({"player_id": [1, 2], "team_id": ["RM", "BCA"]}),
            uri,
            None,
            _snapshot_spec(delete_policy=DeletePolicy.CLOSE),
            _Params(run_date=date(2024, 1, 1)),
        )
        eng.apply(
            pl.DataFrame({"player_id": [1], "team_id": ["RM"]}),
            uri,
            None,
            _snapshot_spec(delete_policy=DeletePolicy.CLOSE),
            _Params(run_date=date(2024, 6, 1)),
        )
        p2 = _read(uri).filter(pl.col("player_id") == 2)
        assert p2["valid_to"].to_list() == [date(2024, 5, 31)]

    def test_ignore_leaves_vector_open(self, tmp_path: Path) -> None:
        uri = _uri(tmp_path)
        eng = _eng()
        eng.apply(
            pl.DataFrame({"player_id": [1, 2], "team_id": ["RM", "BCA"]}),
            uri,
            None,
            _snapshot_spec(delete_policy=DeletePolicy.IGNORE),
            _Params(run_date=date(2024, 1, 1)),
        )
        eng.apply(
            pl.DataFrame({"player_id": [1], "team_id": ["RM"]}),
            uri,
            None,
            _snapshot_spec(delete_policy=DeletePolicy.IGNORE),
            _Params(run_date=date(2024, 6, 1)),
        )
        p2 = _read(uri).filter(pl.col("player_id") == 2)
        assert p2["valid_to"].is_null().all()

    def test_soft_delete_sets_deleted_at(self, tmp_path: Path) -> None:
        uri = _uri(tmp_path)
        eng = _eng()
        eng.apply(
            pl.DataFrame({"player_id": [1, 2], "team_id": ["RM", "BCA"]}),
            uri,
            None,
            _snapshot_spec(delete_policy=DeletePolicy.SOFT_DELETE),
            _Params(run_date=date(2024, 1, 1)),
        )
        eng.apply(
            pl.DataFrame({"player_id": [1], "team_id": ["RM"]}),
            uri,
            None,
            _snapshot_spec(delete_policy=DeletePolicy.SOFT_DELETE),
            _Params(run_date=date(2024, 6, 1)),
        )
        p2 = _read(uri).filter(pl.col("player_id") == 2)
        assert p2["deleted_at"].to_list() == [date(2024, 6, 1)]
        assert p2["valid_to"].to_list() == [date(2024, 5, 31)]


# ---------------------------------------------------------------------------
# Loan case — multiple simultaneous open vectors
# ---------------------------------------------------------------------------


class TestLoanCase:
    def test_two_vectors_open_simultaneously(self, tmp_path: Path) -> None:
        uri = _uri(tmp_path)
        frame = pl.DataFrame(
            {"player_id": [1, 1], "team_id": ["RM", "GET"], "role": ["owner", "loan"]}
        )
        spec = _snapshot_spec(keys=("player_id",), track=("team_id", "role"))
        _eng().apply(frame, uri, None, spec, _Params(run_date=date(2024, 1, 1)))
        result = _read(uri)
        assert len(result) == 2
        assert result["valid_to"].is_null().all()

    def test_loan_end_closes_only_loan_vector(self, tmp_path: Path) -> None:
        uri = _uri(tmp_path)
        spec = _snapshot_spec(keys=("player_id",), track=("team_id", "role"))
        eng = _eng()
        eng.apply(
            pl.DataFrame(
                {"player_id": [1, 1], "team_id": ["RM", "GET"], "role": ["owner", "loan"]}
            ),
            uri,
            None,
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        eng.apply(
            pl.DataFrame({"player_id": [1], "team_id": ["RM"], "role": ["owner"]}),
            uri,
            None,
            spec,
            _Params(run_date=date(2024, 6, 1)),
        )
        result = _read(uri)
        assert len(result) == 2
        loan = result.filter(pl.col("role") == "loan")
        assert loan["valid_to"].to_list() == [date(2024, 5, 31)]
        owner = result.filter(pl.col("role") == "owner")
        assert owner["valid_to"].is_null().all()


# ---------------------------------------------------------------------------
# Passive columns
# ---------------------------------------------------------------------------


class TestPassiveColumns:
    def test_passive_change_does_not_trigger_new_row(self, tmp_path: Path) -> None:
        uri = _uri(tmp_path)
        spec = _snapshot_spec(keys=("player_id",), track=("team_id",))
        eng = _eng()
        eng.apply(
            pl.DataFrame({"player_id": [1], "team_id": ["RM"], "salary": [100]}),
            uri,
            None,
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        eng.apply(
            pl.DataFrame({"player_id": [1], "team_id": ["RM"], "salary": [150]}),
            uri,
            None,
            spec,
            _Params(run_date=date(2024, 6, 1)),
        )
        result = _read(uri)
        assert len(result) == 1
        assert result["salary"].to_list() == [100]


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


class TestIdempotency:
    def test_same_day_rerun_stable_row_count(self, tmp_path: Path) -> None:
        uri = _uri(tmp_path)
        eng = _eng()
        eng.apply(
            pl.DataFrame({"player_id": [1], "team_id": ["RM"]}),
            uri,
            None,
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        frame2 = pl.DataFrame({"player_id": [1], "team_id": ["BCA"]})
        eng.apply(frame2, uri, None, _snapshot_spec(), _Params(run_date=date(2024, 6, 1)))
        eng.apply(frame2, uri, None, _snapshot_spec(), _Params(run_date=date(2024, 6, 1)))
        assert len(_read(uri)) == 2

    def test_same_day_rerun_stable_valid_to(self, tmp_path: Path) -> None:
        uri = _uri(tmp_path)
        eng = _eng()
        eng.apply(
            pl.DataFrame({"player_id": [1], "team_id": ["RM"]}),
            uri,
            None,
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        frame2 = pl.DataFrame({"player_id": [1], "team_id": ["BCA"]})
        for _ in range(3):
            eng.apply(frame2, uri, None, _snapshot_spec(), _Params(run_date=date(2024, 6, 1)))
        old = _read(uri).filter(pl.col("team_id") == "RM")
        assert old["valid_to"].to_list() == [date(2024, 5, 31)]


# ---------------------------------------------------------------------------
# Temporal conflict guard
# ---------------------------------------------------------------------------


class TestTemporalGuard:
    def test_backfill_raises_when_disabled(self, tmp_path: Path) -> None:
        uri = _uri(tmp_path)
        eng = _eng()
        eng.apply(
            pl.DataFrame({"player_id": [1], "team_id": ["RM"]}),
            uri,
            None,
            _snapshot_spec(),
            _Params(run_date=date(2024, 6, 1)),
        )
        with pytest.raises(HistorifyTemporalConflictError):
            eng.apply(
                pl.DataFrame({"player_id": [1], "team_id": ["RM"]}),
                uri,
                None,
                _snapshot_spec(allow_temporal_rerun=False),
                _Params(run_date=date(2024, 1, 1)),
            )

    def test_backfill_allowed_when_flag_set(self, tmp_path: Path) -> None:
        uri = _uri(tmp_path)
        eng = _eng()
        eng.apply(
            pl.DataFrame({"player_id": [1], "team_id": ["RM"]}),
            uri,
            None,
            _snapshot_spec(),
            _Params(run_date=date(2024, 6, 1)),
        )
        eng.apply(
            pl.DataFrame({"player_id": [1], "team_id": ["RM"]}),
            uri,
            None,
            _snapshot_spec(allow_temporal_rerun=True),
            _Params(run_date=date(2024, 1, 1)),
        )


# ---------------------------------------------------------------------------
# LOG mode
# ---------------------------------------------------------------------------


class TestLogBootstrap:
    def test_events_get_valid_from(self, tmp_path: Path) -> None:
        uri = _uri(tmp_path, "dim_subs")
        frame = pl.DataFrame(
            {
                "subscription_id": [1, 1],
                "plan": ["basic", "pro"],
                "event_date": [date(2024, 1, 1), date(2024, 6, 1)],
            }
        )
        _eng().apply(frame, uri, None, _log_spec(), None)
        result = _read(uri).sort("valid_from")
        assert result["valid_from"].to_list() == [date(2024, 1, 1), date(2024, 6, 1)]

    def test_first_event_closed_by_second(self, tmp_path: Path) -> None:
        uri = _uri(tmp_path, "dim_subs")
        frame = pl.DataFrame(
            {
                "subscription_id": [1, 1],
                "plan": ["basic", "pro"],
                "event_date": [date(2024, 1, 1), date(2024, 6, 1)],
            }
        )
        _eng().apply(frame, uri, None, _log_spec(), None)
        basic = _read(uri).filter(pl.col("plan") == "basic")
        assert basic["valid_to"].to_list() == [date(2024, 5, 31)]

    def test_last_event_is_open(self, tmp_path: Path) -> None:
        uri = _uri(tmp_path, "dim_subs")
        frame = pl.DataFrame(
            {
                "subscription_id": [1, 1],
                "plan": ["basic", "pro"],
                "event_date": [date(2024, 1, 1), date(2024, 6, 1)],
            }
        )
        _eng().apply(frame, uri, None, _log_spec(), None)
        pro = _read(uri).filter(pl.col("plan") == "pro")
        assert pro["valid_to"].is_null().all()


class TestLogIncremental:
    def test_new_event_closes_previous(self, tmp_path: Path) -> None:
        uri = _uri(tmp_path, "dim_subs")
        eng = _eng()
        eng.apply(
            pl.DataFrame(
                {"subscription_id": [1], "plan": ["basic"], "event_date": [date(2024, 1, 1)]}
            ),
            uri,
            None,
            _log_spec(),
            None,
        )
        eng.apply(
            pl.DataFrame(
                {"subscription_id": [1], "plan": ["pro"], "event_date": [date(2024, 6, 1)]}
            ),
            uri,
            None,
            _log_spec(),
            None,
        )
        result = _read(uri)
        assert len(result) == 2
        basic = result.filter(pl.col("plan") == "basic")
        assert basic["valid_to"].to_list() == [date(2024, 5, 31)]

    def test_unaffected_entities_preserved(self, tmp_path: Path) -> None:
        uri = _uri(tmp_path, "dim_subs")
        eng = _eng()
        eng.apply(
            pl.DataFrame(
                {
                    "subscription_id": [1, 2],
                    "plan": ["basic", "pro"],
                    "event_date": [date(2024, 1, 1), date(2024, 1, 1)],
                }
            ),
            uri,
            None,
            _log_spec(),
            None,
        )
        eng.apply(
            pl.DataFrame(
                {"subscription_id": [1], "plan": ["enterprise"], "event_date": [date(2024, 9, 1)]}
            ),
            uri,
            None,
            _log_spec(),
            None,
        )
        sub2 = _read(uri).filter(pl.col("subscription_id") == 2)
        assert len(sub2) == 1
        assert sub2["valid_to"].is_null().all()


# ---------------------------------------------------------------------------
# Pure helper unit tests
# ---------------------------------------------------------------------------


class TestResolveTrackCols:
    def test_explicit_track_returned(self) -> None:
        spec = _snapshot_spec(track=("team_id", "salary"))
        result = _resolve_track_cols(spec, ["player_id", "team_id", "salary", "note"])
        assert result == ("team_id", "salary")

    def test_none_track_excludes_keys_and_history(self) -> None:
        spec = _snapshot_spec(keys=("player_id",), track=None)
        result = _resolve_track_cols(spec, ["player_id", "team_id", "salary"])
        assert set(result) == {"team_id", "salary"}


class TestAssertUniqueEntityState:
    def test_unique_frame_passes(self) -> None:
        frame = pl.DataFrame({"player_id": [1, 2], "team_id": ["RM", "BCA"]})
        _assert_unique_entity_state(frame, ["player_id", "team_id"])

    def test_duplicate_raises(self) -> None:
        frame = pl.DataFrame({"player_id": [1, 1], "team_id": ["RM", "RM"]})
        with pytest.raises(HistorifyKeyConflictError):
            _assert_unique_entity_state(frame, ["player_id", "team_id"])


class TestAssertNoDateCollisions:
    def test_unique_events_pass(self) -> None:
        frame = pl.DataFrame(
            {
                "subscription_id": [1, 1],
                "plan": ["a", "b"],
                "event_date": [date(2024, 1, 1), date(2024, 6, 1)],
            }
        )
        _assert_no_date_collisions(frame, ["subscription_id", "plan"], "event_date", _log_spec())

    def test_same_date_same_key_raises(self) -> None:
        frame = pl.DataFrame(
            {
                "subscription_id": [1, 1],
                "plan": ["pro", "pro"],
                "event_date": [date(2024, 1, 1), date(2024, 1, 1)],
            }
        )
        with pytest.raises(HistorifyDateCollisionError):
            _assert_no_date_collisions(
                frame, ["subscription_id", "plan"], "event_date", _log_spec()
            )


class TestStampNewRows:
    def test_adds_history_columns(self) -> None:
        frame = pl.DataFrame({"player_id": [1], "team_id": ["RM"]})
        spec = _snapshot_spec()
        result = _stamp_new_rows(frame, spec, date(2024, 1, 1))
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
        result = _idempotency_strip(existing, spec, date(2024, 6, 1), ["player_id", "team_id"])
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
        result = _idempotency_strip(existing, spec, date(2024, 6, 1), ["player_id", "team_id"])
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
        result = _idempotency_strip(existing, spec, date(2024, 6, 1), ["player_id", "team_id"])
        old = result.filter(pl.col("team_id") == "OLD")
        assert old["valid_to"].to_list() == [date(2024, 3, 31)]
