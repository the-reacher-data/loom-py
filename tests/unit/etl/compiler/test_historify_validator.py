"""Unit tests for compile-time IntoHistory validators.

Covers:
* validate_historify_spec — keys non-empty, track/keys overlap
* validate_param_exprs — effective_date ParamExpr resolves to known field
* Non-historify specs are silently skipped
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from loom.etl import ETLParams, ETLStep, IntoHistory, IntoTable
from loom.etl.compiler import ETLCompilationError
from loom.etl.compiler._errors import ETLErrorCode
from loom.etl.compiler._plan import SourceBinding, TargetBinding
from loom.etl.compiler._validators import validate_historify_spec, validate_param_exprs
from loom.etl.declarative.expr._params import params as p
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target._history import (
    DeletePolicy,
    HistorifyInputMode,
    HistorifySpec,
)
from loom.etl.declarative.target._schema_mode import SchemaMode
from loom.etl.declarative.target._table import AppendSpec

# ---------------------------------------------------------------------------
# Params types for use in tests
# ---------------------------------------------------------------------------


class _DailyParams(ETLParams):
    run_date: date
    season: str


class _MinimalParams(ETLParams):
    pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _history_spec(
    ref: str = "wh.dim_players",
    keys: tuple[str, ...] = ("player_id",),
    track: tuple[str, ...] | None = ("team_id",),
    effective_date: Any = p.run_date,
) -> HistorifySpec:
    return HistorifySpec(
        table_ref=TableRef(ref),
        keys=keys,
        effective_date=effective_date,
        mode=HistorifyInputMode.SNAPSHOT,
        track=track,
        delete_policy=DeletePolicy.CLOSE,
        schema_mode=SchemaMode.STRICT,
    )


def _target(spec: Any) -> TargetBinding:
    return TargetBinding(spec=spec)


def _no_sources() -> tuple[SourceBinding, ...]:
    return ()


# ---------------------------------------------------------------------------
# validate_historify_spec — non-historify specs skipped
# ---------------------------------------------------------------------------


class TestValidateHistorifySpecSkipsOtherSpecs:
    def test_append_spec_skipped(self) -> None:
        spec = AppendSpec(table_ref=TableRef("t.out"))
        validate_historify_spec(object, spec)  # type: ignore[arg-type]

    def test_into_table_upsert_skipped(self) -> None:
        spec = IntoTable("t.out").upsert(keys=("id",))._to_spec()
        validate_historify_spec(object, spec)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# validate_historify_spec — valid specs pass
# ---------------------------------------------------------------------------


class TestValidateHistorifySpecValid:
    def test_valid_spec_passes(self) -> None:
        spec = _history_spec()
        validate_historify_spec(object, spec)  # type: ignore[arg-type]

    def test_multiple_keys_pass(self) -> None:
        spec = _history_spec(keys=("player_id", "league_id"), track=("team_id",))
        validate_historify_spec(object, spec)  # type: ignore[arg-type]

    def test_track_none_passes(self) -> None:
        spec = _history_spec(track=None)
        validate_historify_spec(object, spec)  # type: ignore[arg-type]

    def test_string_effective_date_passes(self) -> None:
        spec = _history_spec(effective_date="event_date")
        validate_historify_spec(object, spec)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# validate_historify_spec — error: empty keys
# ---------------------------------------------------------------------------


class TestValidateHistorifySpecEmptyKeys:
    def test_empty_keys_raises(self) -> None:
        spec = HistorifySpec(
            table_ref=TableRef("wh.dim"),
            keys=(),
            effective_date=p.run_date,
            mode=HistorifyInputMode.SNAPSHOT,
        )
        with pytest.raises(ETLCompilationError, match="at least one column"):
            validate_historify_spec(object, spec)  # type: ignore[arg-type]

    def test_error_code_is_invalid_target_type(self) -> None:
        spec = HistorifySpec(
            table_ref=TableRef("wh.dim"),
            keys=(),
            effective_date=p.run_date,
            mode=HistorifyInputMode.SNAPSHOT,
        )
        with pytest.raises(ETLCompilationError) as exc_info:
            validate_historify_spec(object, spec)  # type: ignore[arg-type]
        assert exc_info.value.code == ETLErrorCode.INVALID_TARGET_TYPE

    def test_error_field_is_keys(self) -> None:
        spec = HistorifySpec(
            table_ref=TableRef("wh.dim"),
            keys=(),
            effective_date=p.run_date,
            mode=HistorifyInputMode.SNAPSHOT,
        )
        with pytest.raises(ETLCompilationError) as exc_info:
            validate_historify_spec(object, spec)  # type: ignore[arg-type]
        assert exc_info.value.field == "keys"


# ---------------------------------------------------------------------------
# validate_historify_spec — error: keys / track overlap
# ---------------------------------------------------------------------------


class TestValidateHistorifySpecTrackOverlap:
    def test_overlap_raises(self) -> None:
        spec = HistorifySpec(
            table_ref=TableRef("wh.dim"),
            keys=("player_id", "team_id"),
            track=("team_id", "salary"),
            effective_date=p.run_date,
            mode=HistorifyInputMode.SNAPSHOT,
        )
        with pytest.raises(ETLCompilationError, match="team_id"):
            validate_historify_spec(object, spec)  # type: ignore[arg-type]

    def test_overlap_error_field_is_track(self) -> None:
        spec = HistorifySpec(
            table_ref=TableRef("wh.dim"),
            keys=("player_id", "team_id"),
            track=("team_id",),
            effective_date=p.run_date,
            mode=HistorifyInputMode.SNAPSHOT,
        )
        with pytest.raises(ETLCompilationError) as exc_info:
            validate_historify_spec(object, spec)  # type: ignore[arg-type]
        assert exc_info.value.field == "track"


# ---------------------------------------------------------------------------
# validate_param_exprs — effective_date as ParamExpr
# ---------------------------------------------------------------------------


class TestValidateParamExprsHistorify:
    """validate_param_exprs must extract effective_date from HistorifySpec."""

    def test_valid_param_expr_passes(self) -> None:
        spec = _history_spec(effective_date=p.run_date)
        validate_param_exprs(object, _DailyParams, _no_sources(), _target(spec))  # type: ignore[arg-type]

    def test_unknown_param_field_raises(self) -> None:
        spec = _history_spec(effective_date=p.snapshot_date)  # not in _DailyParams
        with pytest.raises(ETLCompilationError) as exc_info:
            validate_param_exprs(object, _DailyParams, _no_sources(), _target(spec))  # type: ignore[arg-type]
        assert exc_info.value.code == ETLErrorCode.UNKNOWN_PARAM_FIELD
        assert exc_info.value.field == "snapshot_date"

    def test_string_effective_date_not_validated(self) -> None:
        """String effective_date (LOG mode) is a column name, not a ParamExpr."""
        spec = _history_spec(effective_date="event_date")
        validate_param_exprs(object, _MinimalParams, _no_sources(), _target(spec))  # type: ignore[arg-type]

    def test_nested_param_expr_path_validates_root(self) -> None:
        """params.run_date.year — root field 'run_date' is validated."""
        spec = _history_spec(effective_date=p.run_date.year)
        validate_param_exprs(object, _DailyParams, _no_sources(), _target(spec))  # type: ignore[arg-type]

    def test_non_struct_params_skips_validation(self) -> None:
        """Non-msgspec params type → validation is skipped gracefully."""

        class _PlainParams:
            run_date: date

        spec = _history_spec(effective_date=p.anything_goes)
        validate_param_exprs(object, _PlainParams, _no_sources(), _target(spec))  # type: ignore[arg-type]

    def test_non_historify_spec_not_affected(self) -> None:
        """Existing specs are unaffected by the new historify branch."""
        append_spec = AppendSpec(table_ref=TableRef("t.out"))
        validate_param_exprs(object, _DailyParams, _no_sources(), _target(append_spec))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Full validate_step integration (via ETLStep declaration)
# ---------------------------------------------------------------------------


class _PlayerDimStep(ETLStep[_DailyParams]):
    target = IntoHistory(
        "warehouse.dim_players",
        keys=("player_id",),
        track=("team_id", "salary"),
        effective_date=p.run_date,
        mode="snapshot",
    )

    def execute(self, params: _DailyParams) -> Any:
        return None


class _LoanStep(ETLStep[_DailyParams]):
    """Multiple open vectors — player belongs to two clubs simultaneously."""

    target = IntoHistory(
        "warehouse.dim_contracts",
        keys=("player_id",),
        track=("team_id", "role"),
        effective_date=p.run_date,
        mode="snapshot",
    )

    def execute(self, params: _DailyParams) -> Any:
        return None


class _LogModeStep(ETLStep[_DailyParams]):
    """LOG mode — effective_date is a column name, not a param."""

    target = IntoHistory(
        "warehouse.dim_subscriptions",
        keys=("subscription_id",),
        track=("plan",),
        effective_date="event_date",
        mode="log",
    )

    def execute(self, params: _DailyParams) -> Any:
        return None


class TestValidateStepIntegration:
    """End-to-end: validate_step accepts valid IntoHistory declarations."""

    def test_player_dim_step_compiles(self) -> None:
        from loom.etl.compiler import ETLCompiler
        from loom.etl.testing import StubCatalog

        catalog = StubCatalog(tables={"warehouse.dim_players": ()})
        compiler = ETLCompiler(catalog=catalog)
        compiler.compile_step(_PlayerDimStep)

    def test_loan_step_compiles(self) -> None:
        from loom.etl.compiler import ETLCompiler
        from loom.etl.testing import StubCatalog

        catalog = StubCatalog(tables={"warehouse.dim_contracts": ()})
        compiler = ETLCompiler(catalog=catalog)
        compiler.compile_step(_LoanStep)

    def test_log_mode_step_compiles(self) -> None:
        from loom.etl.compiler import ETLCompiler
        from loom.etl.testing import StubCatalog

        catalog = StubCatalog(tables={"warehouse.dim_subscriptions": ()})
        compiler = ETLCompiler(catalog=catalog)
        compiler.compile_step(_LogModeStep)
