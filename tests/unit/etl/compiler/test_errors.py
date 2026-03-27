"""Unit tests for ETLCompilationError structured fields and ETLErrorCode."""

from __future__ import annotations

import pytest

from loom.etl.compiler._errors import ETLCompilationError, ETLErrorCode


class _Step:
    __qualname__ = "MyStep"


class _Component:
    __qualname__ = "MyComponent"


class _Params:
    __name__ = "_Params"


class _Context:
    __name__ = "ContextParams"


# ---------------------------------------------------------------------------
# ETLErrorCode
# ---------------------------------------------------------------------------


class TestETLErrorCode:
    def test_is_string(self) -> None:
        assert ETLErrorCode.INVALID_PARAMS_TYPE == "INVALID_PARAMS_TYPE"

    def test_all_values_unique(self) -> None:
        values = [c.value for c in ETLErrorCode]
        assert len(values) == len(set(values))


# ---------------------------------------------------------------------------
# Factory: execute() params errors
# ---------------------------------------------------------------------------


class TestInvalidParamsType:
    err = ETLCompilationError.invalid_params_type(_Step, _Params)  # type: ignore[arg-type]

    def test_code(self) -> None:
        assert self.err.code is ETLErrorCode.INVALID_PARAMS_TYPE

    def test_component(self) -> None:
        assert self.err.component == "MyStep"

    def test_field(self) -> None:
        assert self.err.field == "params"

    def test_message_contains_params_type(self) -> None:
        assert "_Params" in str(self.err)


class TestInvalidParamsName:
    err = ETLCompilationError.invalid_params_name(_Step, "ctx")  # type: ignore[arg-type]

    def test_code(self) -> None:
        assert self.err.code is ETLErrorCode.INVALID_PARAMS_NAME

    def test_field_is_the_wrong_name(self) -> None:
        assert self.err.field == "ctx"


class TestMissingSourceParams:
    err = ETLCompilationError.missing_source_params(_Step, frozenset({"orders"}))  # type: ignore[arg-type]

    def test_code(self) -> None:
        assert self.err.code is ETLErrorCode.MISSING_SOURCE_PARAMS

    def test_message_contains_alias(self) -> None:
        assert "orders" in str(self.err)


class TestExtraSourceParams:
    err = ETLCompilationError.extra_source_params(_Step, frozenset({"ghost"}))  # type: ignore[arg-type]

    def test_code(self) -> None:
        assert self.err.code is ETLErrorCode.EXTRA_SOURCE_PARAMS

    def test_message_contains_param_name(self) -> None:
        assert "ghost" in str(self.err)


class TestMissingParamsFields:
    err = ETLCompilationError.missing_params_fields(  # type: ignore[arg-type]
        _Component, frozenset({"run_date"}), _Context
    )

    def test_code(self) -> None:
        assert self.err.code is ETLErrorCode.MISSING_PARAMS_FIELDS

    def test_component(self) -> None:
        assert self.err.component == "MyComponent"

    def test_message_contains_missing_field(self) -> None:
        assert "run_date" in str(self.err)


# ---------------------------------------------------------------------------
# Factory: catalog errors
# ---------------------------------------------------------------------------


class TestUnknownSourceTable:
    err = ETLCompilationError.unknown_source_table(_Step, "orders", "raw.orders")  # type: ignore[arg-type]

    def test_code(self) -> None:
        assert self.err.code is ETLErrorCode.UNKNOWN_SOURCE_TABLE

    def test_field_is_alias(self) -> None:
        assert self.err.field == "orders"

    def test_message_contains_table_ref(self) -> None:
        assert "raw.orders" in str(self.err)


class TestUnknownTargetTable:
    err = ETLCompilationError.unknown_target_table(_Step, "staging.out")  # type: ignore[arg-type]

    def test_code(self) -> None:
        assert self.err.code is ETLErrorCode.UNKNOWN_TARGET_TABLE

    def test_field_is_none(self) -> None:
        assert self.err.field is None


# ---------------------------------------------------------------------------
# Factory: temp errors
# ---------------------------------------------------------------------------


class TestTempNotProduced:
    err = ETLCompilationError.temp_not_produced(_Step, "tmp_orders", "orders_temp")  # type: ignore[arg-type]

    def test_code(self) -> None:
        assert self.err.code is ETLErrorCode.TEMP_NOT_PRODUCED

    def test_field_is_alias(self) -> None:
        assert self.err.field == "tmp_orders"

    def test_message_contains_temp_name(self) -> None:
        assert "orders_temp" in str(self.err)


class TestDuplicateTempName:
    err = ETLCompilationError.duplicate_temp_name(_Step, "orders_temp")  # type: ignore[arg-type]

    def test_code(self) -> None:
        assert self.err.code is ETLErrorCode.DUPLICATE_TEMP_NAME

    def test_field_is_temp_name(self) -> None:
        assert self.err.field == "orders_temp"


class TestInvalidTempAppendMix:
    err = ETLCompilationError.invalid_temp_append_mix(_Step, "orders_temp")  # type: ignore[arg-type]

    def test_code(self) -> None:
        assert self.err.code is ETLErrorCode.INVALID_TEMP_APPEND_MIX

    def test_field_is_temp_name(self) -> None:
        assert self.err.field == "orders_temp"


# ---------------------------------------------------------------------------
# Factory: upsert errors
# ---------------------------------------------------------------------------


class TestUpsertNoKeys:
    err = ETLCompilationError.upsert_no_keys(_Step)  # type: ignore[arg-type]

    def test_code(self) -> None:
        assert self.err.code is ETLErrorCode.UPSERT_NO_KEYS

    def test_is_exception(self) -> None:
        assert isinstance(self.err, Exception)

    def test_str_is_message(self) -> None:
        assert str(self.err) == self.err.args[0]


class TestUpsertExcludeIncludeConflict:
    err = ETLCompilationError.upsert_exclude_include_conflict(_Step)  # type: ignore[arg-type]

    def test_code(self) -> None:
        assert self.err.code is ETLErrorCode.UPSERT_EXCLUDE_INCLUDE_CONFLICT


class TestUpsertKeyInExclude:
    err = ETLCompilationError.upsert_key_in_exclude(_Step, frozenset({"id"}))  # type: ignore[arg-type]

    def test_code(self) -> None:
        assert self.err.code is ETLErrorCode.UPSERT_KEY_IN_EXCLUDE

    def test_message_contains_overlap(self) -> None:
        assert "id" in str(self.err)


# ---------------------------------------------------------------------------
# Raisable as exception
# ---------------------------------------------------------------------------


class TestRaisable:
    @pytest.mark.parametrize(
        "err",
        [
            ETLCompilationError.upsert_no_keys(_Step),  # type: ignore[arg-type]
            ETLCompilationError.missing_target(_Step),  # type: ignore[arg-type]
            ETLCompilationError.temp_not_produced(_Step, "a", "b"),  # type: ignore[arg-type]
        ],
    )
    def test_can_be_raised_and_caught(self, err: ETLCompilationError) -> None:
        with pytest.raises(ETLCompilationError):
            raise err
