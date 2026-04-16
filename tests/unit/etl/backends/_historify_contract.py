"""Shared behavioral contract for SCD Type 2 historify integration tests.

All backend-agnostic scenario tests live here as a single class.
Backend-specific test classes inherit :class:`HistorifyContractTests` and
provide four pytest fixtures:

* ``writer``     — :class:`~loom.etl.runtime.contracts.TargetWriter` rooted
                   at the test's temporary directory.
* ``root``       — :class:`~pathlib.Path` of that temporary directory (used
                   to build expected table URIs in assertions).
* ``make_frame`` — callable ``(list[dict]) → backend-native frame``.
* ``read_table`` — callable ``(uri: str) → list[dict[str, Any]]`` (rows as
                   plain Python dicts with native-typed values).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from loom.etl.declarative.expr._params import params as p
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target._history import (
    DeletePolicy,
    HistorifyInputMode,
    HistorifySpec,
    HistorifyTemporalConflictError,
)
from loom.etl.declarative.target._schema_mode import SchemaMode

# ---------------------------------------------------------------------------
# Shared helpers (module-level — imported by concrete test modules too)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Abstract behavioral contract
# ---------------------------------------------------------------------------


class HistorifyContractTests:
    """Behavioral contract for SCD Type 2 historify across all backends.

    Concrete subclasses must provide the four fixtures below.  Pytest
    discovers and runs every ``test_*`` method defined here as part of each
    concrete subclass.

    Required fixtures
    -----------------
    writer : TargetWriter
        Writer rooted at the test's temporary directory.
    root : Path
        The same temporary directory used by ``writer``.
    make_frame : Callable[[list[dict]], frame]
        Factory that converts a list of row-dicts to a backend frame.
    read_table : Callable[[str], list[dict[str, Any]]]
        Reads a Delta table at the given URI and returns rows as plain dicts.
    """

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _uri(self, root: Path, name: str = "dim_players") -> str:
        return str(root / name)

    # ------------------------------------------------------------------
    # Bootstrap
    # ------------------------------------------------------------------

    def test_creates_table(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        frame = make_frame([{"player_id": 1, "team_id": "RM"}, {"player_id": 2, "team_id": "BCA"}])
        writer.write(frame, _snapshot_spec(), _Params(run_date=date(2024, 1, 1)))
        assert len(read_table(self._uri(root))) == 2

    def test_valid_from_equals_eff_date(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        frame = make_frame([{"player_id": 1, "team_id": "RM"}])
        writer.write(frame, _snapshot_spec(), _Params(run_date=date(2024, 1, 1)))
        rows = read_table(self._uri(root))
        assert rows[0]["valid_from"] == date(2024, 1, 1)

    def test_valid_to_is_null_on_first_write(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        frame = make_frame([{"player_id": 1, "team_id": "RM"}, {"player_id": 2, "team_id": "BCA"}])
        writer.write(frame, _snapshot_spec(), _Params(run_date=date(2024, 1, 1)))
        rows = read_table(self._uri(root))
        assert all(r["valid_to"] is None for r in rows)

    # ------------------------------------------------------------------
    # No-op (no tracked changes)
    # ------------------------------------------------------------------

    def test_no_new_rows_when_unchanged(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        frame = make_frame([{"player_id": 1, "team_id": "RM"}])
        writer.write(frame, _snapshot_spec(), _Params(run_date=date(2024, 1, 1)))
        writer.write(frame, _snapshot_spec(), _Params(run_date=date(2024, 1, 2)))
        rows = read_table(self._uri(root))
        assert len(rows) == 1
        assert rows[0]["valid_to"] is None

    def test_valid_from_preserved_on_no_change(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        frame = make_frame([{"player_id": 1, "team_id": "RM"}])
        writer.write(frame, _snapshot_spec(), _Params(run_date=date(2024, 1, 1)))
        writer.write(frame, _snapshot_spec(), _Params(run_date=date(2024, 1, 2)))
        rows = read_table(self._uri(root))
        assert rows[0]["valid_from"] == date(2024, 1, 1)

    # ------------------------------------------------------------------
    # New entity
    # ------------------------------------------------------------------

    def test_new_player_inserted(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}, {"player_id": 2, "team_id": "BCA"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 5)),
        )
        assert len(read_table(self._uri(root))) == 2

    def test_new_player_valid_from_is_eff_date(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}, {"player_id": 2, "team_id": "BCA"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 5)),
        )
        rows = read_table(self._uri(root))
        p2 = [r for r in rows if r["player_id"] == 2]
        assert p2[0]["valid_from"] == date(2024, 1, 5)
        assert p2[0]["valid_to"] is None

    # ------------------------------------------------------------------
    # Tracked column change
    # ------------------------------------------------------------------

    def test_two_rows_after_team_change(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "BCA"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 6, 1)),
        )
        assert len(read_table(self._uri(root))) == 2

    def test_old_row_valid_to_is_prev_day(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "BCA"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = read_table(self._uri(root))
        old = [r for r in rows if r["team_id"] == "RM"]
        assert old[0]["valid_to"] == date(2024, 5, 31)

    def test_new_row_is_open_after_change(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "BCA"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = read_table(self._uri(root))
        new = [r for r in rows if r["team_id"] == "BCA"]
        assert new[0]["valid_from"] == date(2024, 6, 1)
        assert new[0]["valid_to"] is None

    # ------------------------------------------------------------------
    # Delete policies
    # ------------------------------------------------------------------

    def test_close_sets_valid_to_on_deleted_entity(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        spec = _snapshot_spec(delete_policy=DeletePolicy.CLOSE)
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}, {"player_id": 2, "team_id": "BCA"}]),
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            spec,
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = read_table(self._uri(root))
        p2 = [r for r in rows if r["player_id"] == 2]
        assert p2[0]["valid_to"] == date(2024, 5, 31)

    def test_ignore_leaves_deleted_vector_open(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        spec = _snapshot_spec(delete_policy=DeletePolicy.IGNORE)
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}, {"player_id": 2, "team_id": "BCA"}]),
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            spec,
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = read_table(self._uri(root))
        p2 = [r for r in rows if r["player_id"] == 2]
        assert p2[0]["valid_to"] is None

    def test_soft_delete_sets_deleted_at(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        spec = _snapshot_spec(delete_policy=DeletePolicy.SOFT_DELETE)
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}, {"player_id": 2, "team_id": "BCA"}]),
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            spec,
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = read_table(self._uri(root))
        p2 = [r for r in rows if r["player_id"] == 2]
        assert p2[0]["deleted_at"] == date(2024, 6, 1)
        assert p2[0]["valid_to"] == date(2024, 5, 31)

    # ------------------------------------------------------------------
    # Loan case — multiple simultaneous open vectors
    # ------------------------------------------------------------------

    def test_two_vectors_open_simultaneously(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        spec = _snapshot_spec(keys=("player_id",), track=("team_id", "role"))
        frame = make_frame(
            [
                {"player_id": 1, "team_id": "RM", "role": "owner"},
                {"player_id": 1, "team_id": "GET", "role": "loan"},
            ]
        )
        writer.write(frame, spec, _Params(run_date=date(2024, 1, 1)))
        rows = read_table(self._uri(root))
        assert len(rows) == 2
        assert all(r["valid_to"] is None for r in rows)

    def test_loan_end_closes_only_loan_vector(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        spec = _snapshot_spec(keys=("player_id",), track=("team_id", "role"))
        writer.write(
            make_frame(
                [
                    {"player_id": 1, "team_id": "RM", "role": "owner"},
                    {"player_id": 1, "team_id": "GET", "role": "loan"},
                ]
            ),
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM", "role": "owner"}]),
            spec,
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = read_table(self._uri(root))
        assert len(rows) == 2
        loan = [r for r in rows if r["role"] == "loan"]
        assert loan[0]["valid_to"] == date(2024, 5, 31)
        owner = [r for r in rows if r["role"] == "owner"]
        assert owner[0]["valid_to"] is None

    def test_both_vectors_change_simultaneously(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        """Both open vectors of an entity change on the same run date.

        Expected: 4 rows — 2 closed (old teams), 2 open (new teams).
        This verifies the engine handles multi-vector close+open atomically.
        """
        spec = _snapshot_spec(keys=("player_id",), track=("team_id", "role"))
        writer.write(
            make_frame(
                [
                    {"player_id": 1, "team_id": "RM", "role": "owner"},
                    {"player_id": 1, "team_id": "GET", "role": "loan"},
                ]
            ),
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame(
                [
                    {"player_id": 1, "team_id": "MCF", "role": "owner"},
                    {"player_id": 1, "team_id": "BAR", "role": "loan"},
                ]
            ),
            spec,
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = read_table(self._uri(root))
        assert len(rows) == 4
        open_rows = [r for r in rows if r["valid_to"] is None]
        closed_rows = [r for r in rows if r["valid_to"] is not None]
        assert len(open_rows) == 2
        assert len(closed_rows) == 2
        assert all(r["valid_to"] == date(2024, 5, 31) for r in closed_rows)
        open_teams = {r["team_id"] for r in open_rows}
        assert open_teams == {"MCF", "BAR"}

    def test_third_vector_added_to_entity_with_two_open(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        """Entity gains a third simultaneous vector while two are already open.

        Expected: 3 rows open, no rows closed — existing vectors are unchanged,
        new vector is inserted with its own valid_from.
        """
        spec = _snapshot_spec(keys=("player_id",), track=("team_id", "role"))
        writer.write(
            make_frame(
                [
                    {"player_id": 1, "team_id": "RM", "role": "owner"},
                    {"player_id": 1, "team_id": "GET", "role": "loan"},
                ]
            ),
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame(
                [
                    {"player_id": 1, "team_id": "RM", "role": "owner"},
                    {"player_id": 1, "team_id": "GET", "role": "loan"},
                    {"player_id": 1, "team_id": "SEV", "role": "loan2"},
                ]
            ),
            spec,
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = read_table(self._uri(root))
        assert len(rows) == 3
        assert all(r["valid_to"] is None for r in rows)
        new_vec = [r for r in rows if r["role"] == "loan2"]
        assert new_vec[0]["valid_from"] == date(2024, 6, 1)

    def test_idempotency_stable_with_two_open_vectors(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        """Same-day rerun is stable when entity has two simultaneous open vectors.

        Expected: 4 rows after first change (2 old closed, 2 new open).
        Rerunning on the same date must not add or remove rows.
        """
        spec = _snapshot_spec(keys=("player_id",), track=("team_id", "role"))
        writer.write(
            make_frame(
                [
                    {"player_id": 1, "team_id": "RM", "role": "owner"},
                    {"player_id": 1, "team_id": "GET", "role": "loan"},
                ]
            ),
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        frame2 = make_frame(
            [
                {"player_id": 1, "team_id": "MCF", "role": "owner"},
                {"player_id": 1, "team_id": "BAR", "role": "loan"},
            ]
        )
        writer.write(frame2, spec, _Params(run_date=date(2024, 6, 1)))
        writer.write(frame2, spec, _Params(run_date=date(2024, 6, 1)))  # rerun
        rows = read_table(self._uri(root))
        assert len(rows) == 4

    # ------------------------------------------------------------------
    # Passive columns
    # ------------------------------------------------------------------

    def test_passive_change_does_not_trigger_new_row(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        spec = _snapshot_spec(keys=("player_id",), track=("team_id",))
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM", "salary": 100}]),
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM", "salary": 150}]),
            spec,
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = read_table(self._uri(root))
        assert len(rows) == 1
        assert rows[0]["salary"] == 100

    # ------------------------------------------------------------------
    # Idempotency
    # ------------------------------------------------------------------

    def test_same_day_rerun_stable_row_count(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        frame2 = make_frame([{"player_id": 1, "team_id": "BCA"}])
        writer.write(frame2, _snapshot_spec(), _Params(run_date=date(2024, 6, 1)))
        writer.write(frame2, _snapshot_spec(), _Params(run_date=date(2024, 6, 1)))
        assert len(read_table(self._uri(root))) == 2

    def test_same_day_rerun_stable_valid_to(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        frame2 = make_frame([{"player_id": 1, "team_id": "BCA"}])
        for _ in range(3):
            writer.write(frame2, _snapshot_spec(), _Params(run_date=date(2024, 6, 1)))
        rows = read_table(self._uri(root))
        old = [r for r in rows if r["team_id"] == "RM"]
        assert old[0]["valid_to"] == date(2024, 5, 31)

    # ------------------------------------------------------------------
    # Temporal conflict guard
    # ------------------------------------------------------------------

    def test_backfill_raises_when_disabled(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
    ) -> None:
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 6, 1)),
        )
        with pytest.raises(HistorifyTemporalConflictError):
            writer.write(
                make_frame([{"player_id": 1, "team_id": "RM"}]),
                _snapshot_spec(allow_temporal_rerun=False),
                _Params(run_date=date(2024, 1, 1)),
            )

    def test_backfill_allowed_when_flag_set(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
    ) -> None:
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 6, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(allow_temporal_rerun=True),
            _Params(run_date=date(2024, 1, 1)),
        )

    # ------------------------------------------------------------------
    # LOG mode
    # ------------------------------------------------------------------

    def test_log_events_get_valid_from(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        frame = make_frame(
            [
                {"subscription_id": 1, "plan": "basic", "event_date": date(2024, 1, 1)},
                {"subscription_id": 1, "plan": "pro", "event_date": date(2024, 6, 1)},
            ]
        )
        writer.write(frame, _log_spec(), None)
        rows = sorted(read_table(self._uri(root, "dim_subs")), key=lambda r: r["valid_from"])
        assert [r["valid_from"] for r in rows] == [date(2024, 1, 1), date(2024, 6, 1)]

    def test_log_first_event_closed_by_second(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        frame = make_frame(
            [
                {"subscription_id": 1, "plan": "basic", "event_date": date(2024, 1, 1)},
                {"subscription_id": 1, "plan": "pro", "event_date": date(2024, 6, 1)},
            ]
        )
        writer.write(frame, _log_spec(), None)
        rows = read_table(self._uri(root, "dim_subs"))
        basic = [r for r in rows if r["plan"] == "basic"]
        assert basic[0]["valid_to"] == date(2024, 5, 31)

    def test_log_last_event_is_open(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        frame = make_frame(
            [
                {"subscription_id": 1, "plan": "basic", "event_date": date(2024, 1, 1)},
                {"subscription_id": 1, "plan": "pro", "event_date": date(2024, 6, 1)},
            ]
        )
        writer.write(frame, _log_spec(), None)
        rows = read_table(self._uri(root, "dim_subs"))
        pro = [r for r in rows if r["plan"] == "pro"]
        assert pro[0]["valid_to"] is None

    def test_log_new_event_closes_previous(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        writer.write(
            make_frame([{"subscription_id": 1, "plan": "basic", "event_date": date(2024, 1, 1)}]),
            _log_spec(),
            None,
        )
        writer.write(
            make_frame([{"subscription_id": 1, "plan": "pro", "event_date": date(2024, 6, 1)}]),
            _log_spec(),
            None,
        )
        rows = read_table(self._uri(root, "dim_subs"))
        assert len(rows) == 2
        basic = [r for r in rows if r["plan"] == "basic"]
        assert basic[0]["valid_to"] == date(2024, 5, 31)

    def test_log_unaffected_entities_preserved(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        writer.write(
            make_frame(
                [
                    {"subscription_id": 1, "plan": "basic", "event_date": date(2024, 1, 1)},
                    {"subscription_id": 2, "plan": "pro", "event_date": date(2024, 1, 1)},
                ]
            ),
            _log_spec(),
            None,
        )
        writer.write(
            make_frame(
                [{"subscription_id": 1, "plan": "enterprise", "event_date": date(2024, 9, 1)}]
            ),
            _log_spec(),
            None,
        )
        rows = read_table(self._uri(root, "dim_subs"))
        sub2 = [r for r in rows if r["subscription_id"] == 2]
        assert len(sub2) == 1
        assert sub2[0]["valid_to"] is None
