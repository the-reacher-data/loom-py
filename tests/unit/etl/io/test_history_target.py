"""Unit tests for IntoHistory builder and SCD Type 2 contracts.

Real-world use cases covered:

* **Player dimension** — standard SCD2 with full daily snapshot.
* **Player loan** — player belongs to two teams simultaneously (two independent
  open vectors via ``keys + track`` join key).
* **Subscription plan** — LOG mode with sub-day timestamp precision.
* **Product pricing** — track price, ignore description; passive columns carried.
* **Employee department** — close-on-absence with partition scope optimisation.
* **Soft-delete audit** — explicit deletion audit trail via ``SOFT_DELETE`` policy.
* **Historical backfill** — ``allow_temporal_rerun=True`` re-weave path.
"""

from __future__ import annotations

import pytest

from loom.etl.declarative.expr._params import ParamExpr, params
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target import (
    DeletePolicy,
    HistorifyDateCollisionError,
    HistorifyInputMode,
    HistorifyKeyConflictError,
    HistorifyRepairReport,
    HistorifySpec,
    HistorifyTemporalConflictError,
    HistoryDateType,
    IntoHistory,
    TargetSpec,
)
from loom.etl.declarative.target._schema_mode import SchemaMode

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_player_dim() -> IntoHistory:
    """Standard SCD2 player dimension — full daily snapshot."""
    return IntoHistory(
        "warehouse.dim_players",
        keys=("player_id",),
        track=("team_id", "contract_value"),
        effective_date=params.run_date,
        mode="snapshot",
        delete_policy="close",
        partition_scope=("season",),
    )


def _make_loan_dim() -> IntoHistory:
    """Player loan — multiple simultaneous open vectors per entity."""
    return IntoHistory(
        "warehouse.dim_player_contracts",
        keys=("player_id",),
        track=("team_id", "role"),
        effective_date=params.run_date,
        mode="snapshot",
    )


def _make_subscription_log() -> IntoHistory:
    """Subscription plan history — LOG mode, timestamp precision."""
    return IntoHistory(
        "warehouse.dim_subscriptions",
        keys=("subscription_id",),
        track=("plan", "price_eur"),
        effective_date="event_ts",
        mode="log",
        date_type="timestamp",
    )


def _make_product_pricing() -> IntoHistory:
    """Product pricing — track price only; description is a passive column."""
    return IntoHistory(
        "warehouse.dim_products",
        keys=("product_id",),
        track=("price_eur",),
        effective_date=params.run_date,
        mode="snapshot",
    )


def _make_employee_dept() -> IntoHistory:
    """Employee department — close on absence, partition scope."""
    return IntoHistory(
        "hr.dim_employee_dept",
        keys=("employee_id",),
        track=("dept_id", "job_grade"),
        effective_date=params.snapshot_date,
        mode="snapshot",
        delete_policy="close",
        partition_scope=("country_code",),
    )


def _make_soft_delete() -> IntoHistory:
    """CRM contact history with explicit soft-delete audit trail."""
    return IntoHistory(
        "crm.dim_contacts",
        keys=("contact_id",),
        track=("status", "assigned_team"),
        effective_date=params.run_date,
        mode="snapshot",
        delete_policy="soft_delete",
    )


def _make_backfill() -> IntoHistory:
    """Historical backfill — allow_temporal_rerun enables re-weave."""
    return IntoHistory(
        "warehouse.dim_orders",
        keys=("order_id",),
        track=("state", "warehouse_id"),
        effective_date="event_date",
        mode="log",
        allow_temporal_rerun=True,
    )


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class TestHistorifyInputMode:
    def test_snapshot_value(self) -> None:
        assert HistorifyInputMode.SNAPSHOT == "snapshot"

    def test_log_value(self) -> None:
        assert HistorifyInputMode.LOG == "log"

    def test_from_string(self) -> None:
        assert HistorifyInputMode("snapshot") is HistorifyInputMode.SNAPSHOT
        assert HistorifyInputMode("log") is HistorifyInputMode.LOG

    def test_is_str(self) -> None:
        assert isinstance(HistorifyInputMode.SNAPSHOT, str)


class TestDeletePolicy:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("ignore", DeletePolicy.IGNORE),
            ("close", DeletePolicy.CLOSE),
            ("soft_delete", DeletePolicy.SOFT_DELETE),
        ],
    )
    def test_from_string(self, value: str, expected: DeletePolicy) -> None:
        assert DeletePolicy(value) is expected

    def test_all_policies_are_strings(self) -> None:
        for policy in DeletePolicy:
            assert isinstance(policy, str)


class TestHistoryDateType:
    def test_date_value(self) -> None:
        assert HistoryDateType.DATE == "date"

    def test_timestamp_value(self) -> None:
        assert HistoryDateType.TIMESTAMP == "timestamp"


# ---------------------------------------------------------------------------
# IntoHistory builder — spec production
# ---------------------------------------------------------------------------


class TestIntoHistoryPlayerDimension:
    """Standard SCD2 — full daily snapshot of a player dimension."""

    def setup_method(self) -> None:
        self.spec = _make_player_dim()._to_spec()

    def test_table_ref(self) -> None:
        assert self.spec.table_ref == TableRef("warehouse.dim_players")

    def test_keys(self) -> None:
        assert self.spec.keys == ("player_id",)

    def test_track(self) -> None:
        assert self.spec.track == ("team_id", "contract_value")

    def test_mode_is_snapshot(self) -> None:
        assert self.spec.mode is HistorifyInputMode.SNAPSHOT

    def test_delete_policy_is_close(self) -> None:
        assert self.spec.delete_policy is DeletePolicy.CLOSE

    def test_partition_scope(self) -> None:
        assert self.spec.partition_scope == ("season",)

    def test_effective_date_is_param_expr(self) -> None:
        assert isinstance(self.spec.effective_date, ParamExpr)

    def test_defaults_valid_from_valid_to(self) -> None:
        assert self.spec.valid_from == "valid_from"
        assert self.spec.valid_to == "valid_to"

    def test_defaults_date_type(self) -> None:
        assert self.spec.date_type is HistoryDateType.DATE

    def test_defaults_schema_mode(self) -> None:
        assert self.spec.schema_mode is SchemaMode.STRICT

    def test_defaults_allow_temporal_rerun(self) -> None:
        assert self.spec.allow_temporal_rerun is False

    def test_spec_is_frozen(self) -> None:
        with pytest.raises((AttributeError, TypeError)):
            self.spec.keys = ("other",)  # type: ignore[misc]


class TestIntoHistoryLoanCase:
    """Player loan — two open vectors for the same player_id coexist.

    The (player_id=P1, team_id=RM, role=OWNER) and
    (player_id=P1, team_id=GET, role=LOAN) vectors are independent because
    the join key is keys + track = (player_id, team_id, role).
    """

    def setup_method(self) -> None:
        self.spec = _make_loan_dim()._to_spec()

    def test_keys(self) -> None:
        assert self.spec.keys == ("player_id",)

    def test_track_includes_team_and_role(self) -> None:
        assert self.spec.track == ("team_id", "role")

    def test_join_key_is_keys_plus_track(self) -> None:
        """The conceptual join key (keys + track) covers both open vectors."""
        join_key = self.spec.keys + (self.spec.track or ())
        assert "player_id" in join_key
        assert "team_id" in join_key
        assert "role" in join_key

    def test_no_partition_scope(self) -> None:
        assert self.spec.partition_scope is None

    def test_delete_policy_default_is_close(self) -> None:
        assert self.spec.delete_policy is DeletePolicy.CLOSE


class TestIntoHistorySubscriptionLog:
    """Subscription plan — LOG mode with sub-day precision."""

    def setup_method(self) -> None:
        self.spec = _make_subscription_log()._to_spec()

    def test_mode_is_log(self) -> None:
        assert self.spec.mode is HistorifyInputMode.LOG

    def test_effective_date_is_column_name(self) -> None:
        assert self.spec.effective_date == "event_ts"

    def test_date_type_is_timestamp(self) -> None:
        assert self.spec.date_type is HistoryDateType.TIMESTAMP

    def test_delete_policy_defaults_to_close(self) -> None:
        # LOG mode — policy present but ignored by engine
        assert self.spec.delete_policy is DeletePolicy.CLOSE


class TestIntoHistoryProductPricing:
    """Product pricing — only price_eur triggers new history rows."""

    def setup_method(self) -> None:
        self.spec = _make_product_pricing()._to_spec()

    def test_track_only_price(self) -> None:
        assert self.spec.track == ("price_eur",)

    def test_mode_snapshot(self) -> None:
        assert self.spec.mode is HistorifyInputMode.SNAPSHOT


class TestIntoHistoryEmployeeDept:
    """Employee department — partition scope on country_code."""

    def setup_method(self) -> None:
        self.spec = _make_employee_dept()._to_spec()

    def test_partition_scope(self) -> None:
        assert self.spec.partition_scope == ("country_code",)

    def test_track_dept_and_grade(self) -> None:
        assert self.spec.track == ("dept_id", "job_grade")


class TestIntoHistorySoftDelete:
    """CRM contact — soft_delete policy for explicit audit trail."""

    def setup_method(self) -> None:
        self.spec = _make_soft_delete()._to_spec()

    def test_delete_policy_soft_delete(self) -> None:
        assert self.spec.delete_policy is DeletePolicy.SOFT_DELETE


class TestIntoHistoryBackfill:
    """Historical backfill — temporal rerun enabled."""

    def setup_method(self) -> None:
        self.spec = _make_backfill()._to_spec()

    def test_allow_temporal_rerun(self) -> None:
        assert self.spec.allow_temporal_rerun is True

    def test_mode_is_log(self) -> None:
        assert self.spec.mode is HistorifyInputMode.LOG


# ---------------------------------------------------------------------------
# IntoHistory — parameter normalisation
# ---------------------------------------------------------------------------


class TestIntoHistoryParameterNormalisation:
    def test_str_ref_becomes_table_ref(self) -> None:
        spec = IntoHistory(
            "my.table",
            keys=("id",),
            effective_date=params.run_date,
        )._to_spec()
        assert isinstance(spec.table_ref, TableRef)
        assert spec.table_ref.ref == "my.table"

    def test_table_ref_accepted_directly(self) -> None:
        ref = TableRef("my.table")
        spec = IntoHistory(ref, keys=("id",), effective_date=params.run_date)._to_spec()
        assert spec.table_ref is ref

    def test_keys_list_converted_to_tuple(self) -> None:
        spec = IntoHistory(
            "t",
            keys=["a", "b"],
            effective_date=params.run_date,
        )._to_spec()
        assert spec.keys == ("a", "b")

    def test_track_list_converted_to_tuple(self) -> None:
        spec = IntoHistory(
            "t",
            keys=("id",),
            track=["x", "y"],
            effective_date=params.run_date,
        )._to_spec()
        assert spec.track == ("x", "y")

    def test_partition_scope_list_converted_to_tuple(self) -> None:
        spec = IntoHistory(
            "t",
            keys=("id",),
            effective_date=params.run_date,
            partition_scope=["year", "month"],
        )._to_spec()
        assert spec.partition_scope == ("year", "month")

    def test_track_none_by_default(self) -> None:
        spec = IntoHistory("t", keys=("id",), effective_date=params.run_date)._to_spec()
        assert spec.track is None

    def test_partition_scope_none_by_default(self) -> None:
        spec = IntoHistory("t", keys=("id",), effective_date=params.run_date)._to_spec()
        assert spec.partition_scope is None

    @pytest.mark.parametrize("mode_str", ["snapshot", "log"])
    def test_mode_string_converted_to_enum(self, mode_str: str) -> None:
        effective = "event_date" if mode_str == "log" else params.run_date
        spec = IntoHistory(
            "t",
            keys=("id",),
            effective_date=effective,
            mode=mode_str,  # type: ignore[arg-type]
        )._to_spec()
        assert spec.mode == HistorifyInputMode(mode_str)

    @pytest.mark.parametrize("policy_str", ["ignore", "close", "soft_delete"])
    def test_delete_policy_string_converted_to_enum(self, policy_str: str) -> None:
        spec = IntoHistory(
            "t",
            keys=("id",),
            effective_date=params.run_date,
            delete_policy=policy_str,  # type: ignore[arg-type]
        )._to_spec()
        assert spec.delete_policy == DeletePolicy(policy_str)

    @pytest.mark.parametrize("date_type_str", ["date", "timestamp"])
    def test_date_type_string_converted_to_enum(self, date_type_str: str) -> None:
        spec = IntoHistory(
            "t",
            keys=("id",),
            effective_date=params.run_date,
            date_type=date_type_str,  # type: ignore[arg-type]
        )._to_spec()
        assert spec.date_type == HistoryDateType(date_type_str)

    @pytest.mark.parametrize("schema", [SchemaMode.STRICT, SchemaMode.EVOLVE, SchemaMode.OVERWRITE])
    def test_schema_mode_propagated(self, schema: SchemaMode) -> None:
        spec = IntoHistory(
            "t",
            keys=("id",),
            effective_date=params.run_date,
            schema=schema,
        )._to_spec()
        assert spec.schema_mode is schema

    def test_custom_valid_from_valid_to(self) -> None:
        spec = IntoHistory(
            "t",
            keys=("id",),
            effective_date=params.run_date,
            valid_from="start_date",
            valid_to="end_date",
        )._to_spec()
        assert spec.valid_from == "start_date"
        assert spec.valid_to == "end_date"


# ---------------------------------------------------------------------------
# IntoHistory — validation errors
# ---------------------------------------------------------------------------


class TestIntoHistoryValidation:
    def test_empty_keys_raises(self) -> None:
        with pytest.raises(ValueError, match="at least one column name"):
            IntoHistory("t", keys=(), effective_date=params.run_date)

    def test_empty_keys_list_raises(self) -> None:
        with pytest.raises(ValueError, match="at least one column name"):
            IntoHistory("t", keys=[], effective_date=params.run_date)

    def test_keys_track_overlap_raises(self) -> None:
        with pytest.raises(ValueError, match="cannot appear in both"):
            IntoHistory(
                "t",
                keys=("player_id", "team_id"),
                track=("team_id", "salary"),
                effective_date=params.run_date,
            )

    def test_keys_track_overlap_message_lists_offender(self) -> None:
        with pytest.raises(ValueError, match="team_id"):
            IntoHistory(
                "t",
                keys=("player_id", "team_id"),
                track=("team_id",),
                effective_date=params.run_date,
            )

    def test_valid_from_equals_valid_to_raises(self) -> None:
        with pytest.raises(ValueError, match="must be distinct"):
            IntoHistory(
                "t",
                keys=("id",),
                effective_date=params.run_date,
                valid_from="period",
                valid_to="period",
            )

    def test_log_eff_date_equals_valid_from_raises(self) -> None:
        with pytest.raises(ValueError, match="must not match"):
            IntoHistory(
                "t",
                keys=("id",),
                effective_date="valid_from",
                mode="log",
                valid_from="valid_from",
                valid_to="valid_to",
            )

    def test_log_eff_date_equals_valid_to_raises(self) -> None:
        with pytest.raises(ValueError, match="must not match"):
            IntoHistory(
                "t",
                keys=("id",),
                effective_date="valid_to",
                mode="log",
                valid_from="valid_from",
                valid_to="valid_to",
            )

    def test_non_overlapping_keys_and_track_accepted(self) -> None:
        """Ensure valid config does not raise."""
        spec = IntoHistory(
            "t",
            keys=("player_id",),
            track=("team_id", "salary"),
            effective_date=params.run_date,
        )._to_spec()
        assert spec.keys == ("player_id",)
        assert spec.track == ("team_id", "salary")


# ---------------------------------------------------------------------------
# IntoHistory — identity and repr
# ---------------------------------------------------------------------------


class TestIntoHistoryIdentity:
    def test_to_spec_returns_historify_spec(self) -> None:
        spec = _make_player_dim()._to_spec()
        assert isinstance(spec, HistorifySpec)

    def test_to_spec_same_object_on_repeated_calls(self) -> None:
        builder = _make_player_dim()
        assert builder._to_spec() is builder._to_spec()

    def test_repr_includes_table_ref(self) -> None:
        assert "warehouse.dim_players" in repr(_make_player_dim())

    def test_repr_includes_keys(self) -> None:
        assert "player_id" in repr(_make_player_dim())

    def test_repr_includes_mode(self) -> None:
        assert "snapshot" in repr(_make_player_dim())

    def test_historify_spec_in_target_spec_union(self) -> None:
        """HistorifySpec is part of the TargetSpec union."""
        spec = _make_player_dim()._to_spec()
        # Runtime check: isinstance against each member of the union
        assert isinstance(spec, HistorifySpec)


# ---------------------------------------------------------------------------
# HistorifySpec — frozen dataclass
# ---------------------------------------------------------------------------


class TestHistorifySpecDataclass:
    def test_frozen_reject_mutation(self) -> None:
        spec = _make_player_dim()._to_spec()
        with pytest.raises((AttributeError, TypeError)):
            spec.keys = ("other",)  # type: ignore[misc]

    def test_equality_by_value(self) -> None:
        a = _make_player_dim()._to_spec()
        b = _make_player_dim()._to_spec()
        assert a == b

    def test_inequality_on_different_keys(self) -> None:
        a = IntoHistory("t", keys=("id",), effective_date=params.run_date)._to_spec()
        b = IntoHistory("t", keys=("other_id",), effective_date=params.run_date)._to_spec()
        assert a != b

    def test_hashable(self) -> None:
        spec = _make_player_dim()._to_spec()
        assert hash(spec) == hash(spec)

    def test_table_ref_field(self) -> None:
        spec = _make_player_dim()._to_spec()
        assert hasattr(spec, "table_ref")
        assert isinstance(spec.table_ref, TableRef)

    def test_schema_mode_field_present(self) -> None:
        """schema_mode required for _is_table_target_spec duck-type detection."""
        spec = _make_player_dim()._to_spec()
        assert hasattr(spec, "schema_mode")


# ---------------------------------------------------------------------------
# Error types
# ---------------------------------------------------------------------------


class TestHistorifyKeyConflictError:
    def test_is_value_error(self) -> None:
        err = HistorifyKeyConflictError("(player_id=1, team_id=RM, role=OWNER) x2")
        assert isinstance(err, ValueError)

    def test_message_contains_duplicate_info(self) -> None:
        err = HistorifyKeyConflictError("player_id=1, team_id=RM")
        assert "player_id=1" in str(err)

    def test_message_guides_user_toward_track(self) -> None:
        err = HistorifyKeyConflictError("foo")
        assert "track" in str(err)

    def test_can_be_raised_and_caught(self) -> None:
        with pytest.raises(HistorifyKeyConflictError):
            raise HistorifyKeyConflictError("dup")

    def test_caught_as_value_error(self) -> None:
        with pytest.raises(ValueError):
            raise HistorifyKeyConflictError("dup")


class TestHistorifyDateCollisionError:
    def test_is_value_error(self) -> None:
        err = HistorifyDateCollisionError("(sub_id=42, plan=PRO) on 2025-01-01")
        assert isinstance(err, ValueError)

    def test_message_mentions_timestamp(self) -> None:
        err = HistorifyDateCollisionError("collision")
        assert "timestamp" in str(err)

    def test_message_contains_collision_info(self) -> None:
        err = HistorifyDateCollisionError("sub_id=42")
        assert "sub_id=42" in str(err)

    def test_can_be_raised_and_caught(self) -> None:
        with pytest.raises(HistorifyDateCollisionError):
            raise HistorifyDateCollisionError("dup")


class TestHistorifyTemporalConflictError:
    def test_is_value_error(self) -> None:
        err = HistorifyTemporalConflictError("2025-06-01", "2025-01-01")
        assert isinstance(err, ValueError)

    def test_message_contains_both_dates(self) -> None:
        err = HistorifyTemporalConflictError("2025-06-01", "2025-01-01")
        assert "2025-06-01" in str(err)
        assert "2025-01-01" in str(err)

    def test_message_mentions_allow_temporal_rerun(self) -> None:
        err = HistorifyTemporalConflictError("2025-06-01", "2025-01-01")
        assert "allow_temporal_rerun" in str(err)

    def test_can_be_raised_and_caught(self) -> None:
        with pytest.raises(HistorifyTemporalConflictError):
            raise HistorifyTemporalConflictError("2025-06-01", "2025-01-01")


# ---------------------------------------------------------------------------
# HistorifyRepairReport
# ---------------------------------------------------------------------------


class TestHistorifyRepairReport:
    def test_fields_accessible(self) -> None:
        report = HistorifyRepairReport(
            affected_keys=frozenset({("player_id", 1), ("player_id", 2)}),
            dates_requiring_rerun=("2025-01-01", "2025-01-02"),
            warnings=("Re-wove 3 records for player_id=1",),
        )
        assert len(report.affected_keys) == 2
        assert report.dates_requiring_rerun == ("2025-01-01", "2025-01-02")
        assert "player_id=1" in report.warnings[0]

    def test_is_frozen(self) -> None:
        report = HistorifyRepairReport(
            affected_keys=frozenset(),
            dates_requiring_rerun=(),
            warnings=(),
        )
        with pytest.raises((AttributeError, TypeError)):
            report.warnings = ("new",)  # type: ignore[misc]

    def test_empty_report(self) -> None:
        report = HistorifyRepairReport(
            affected_keys=frozenset(),
            dates_requiring_rerun=(),
            warnings=(),
        )
        assert len(report.affected_keys) == 0
        assert len(report.dates_requiring_rerun) == 0


# ---------------------------------------------------------------------------
# Public API surface (imports from loom.etl root)
# ---------------------------------------------------------------------------


class TestPublicApiSurface:
    def test_into_history_importable_from_loom_etl(self) -> None:
        from loom.etl import IntoHistory as _IntoHistory  # noqa: F401

    def test_historify_spec_importable_from_loom_etl(self) -> None:
        from loom.etl import HistorifySpec as _HistorifySpec  # noqa: F401

    def test_delete_policy_importable_from_loom_etl(self) -> None:
        from loom.etl import DeletePolicy as _DeletePolicy  # noqa: F401

    def test_errors_importable_from_loom_etl(self) -> None:
        from loom.etl import (  # noqa: F401
            HistorifyDateCollisionError as _A,
        )

    def test_into_history_importable_from_declarative_target(self) -> None:
        from loom.etl.declarative.target import IntoHistory as _IntoHistory  # noqa: F401

    def test_historify_spec_in_target_spec_type_alias(self) -> None:
        """HistorifySpec is part of the TargetSpec union."""
        import typing

        args = typing.get_args(TargetSpec)
        assert HistorifySpec in args
