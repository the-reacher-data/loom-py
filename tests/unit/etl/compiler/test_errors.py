"""Unit tests for ETLCompilationError structured fields and ETLErrorCode."""

from __future__ import annotations

from loom.etl.compiler._errors import ETLCompilationError, ETLErrorCode


class _Step:
    __qualname__ = "MyStep"


class _Component:
    __qualname__ = "MyComponent"


class _Params:
    __name__ = "MyParams"


class _Context:
    __name__ = "ContextParams"


# ---------------------------------------------------------------------------
# ETLErrorCode is a StrEnum usable as a string
# ---------------------------------------------------------------------------


def test_error_code_is_string() -> None:
    assert ETLErrorCode.INVALID_PARAMS_TYPE == "INVALID_PARAMS_TYPE"


def test_all_codes_unique() -> None:
    values = [c.value for c in ETLErrorCode]
    assert len(values) == len(set(values))


# ---------------------------------------------------------------------------
# Factory: invalid_params_type
# ---------------------------------------------------------------------------


def test_invalid_params_type_code() -> None:
    err = ETLCompilationError.invalid_params_type(_Step, _Params)  # type: ignore[arg-type]
    assert err.code is ETLErrorCode.INVALID_PARAMS_TYPE


def test_invalid_params_type_component() -> None:
    err = ETLCompilationError.invalid_params_type(_Step, _Params)  # type: ignore[arg-type]
    assert err.component == "MyStep"


def test_invalid_params_type_field() -> None:
    err = ETLCompilationError.invalid_params_type(_Step, _Params)  # type: ignore[arg-type]
    assert err.field == "params"


def test_invalid_params_type_message_contains_name() -> None:
    err = ETLCompilationError.invalid_params_type(_Step, _Params)  # type: ignore[arg-type]
    assert "_Params" in str(err)


# ---------------------------------------------------------------------------
# Factory: invalid_params_name
# ---------------------------------------------------------------------------


def test_invalid_params_name_code() -> None:
    err = ETLCompilationError.invalid_params_name(_Step, "ctx")  # type: ignore[arg-type]
    assert err.code is ETLErrorCode.INVALID_PARAMS_NAME


def test_invalid_params_name_field() -> None:
    err = ETLCompilationError.invalid_params_name(_Step, "ctx")  # type: ignore[arg-type]
    assert err.field == "ctx"


# ---------------------------------------------------------------------------
# Factory: missing_source_params
# ---------------------------------------------------------------------------


def test_missing_source_params_code() -> None:
    err = ETLCompilationError.missing_source_params(_Step, frozenset({"orders"}))  # type: ignore[arg-type]
    assert err.code is ETLErrorCode.MISSING_SOURCE_PARAMS


def test_missing_source_params_field_is_none() -> None:
    err = ETLCompilationError.missing_source_params(_Step, frozenset({"orders"}))  # type: ignore[arg-type]
    assert err.field is None


def test_missing_source_params_message_contains_alias() -> None:
    err = ETLCompilationError.missing_source_params(_Step, frozenset({"orders"}))  # type: ignore[arg-type]
    assert "orders" in str(err)


# ---------------------------------------------------------------------------
# Factory: extra_source_params
# ---------------------------------------------------------------------------


def test_extra_source_params_code() -> None:
    err = ETLCompilationError.extra_source_params(_Step, frozenset({"ghost"}))  # type: ignore[arg-type]
    assert err.code is ETLErrorCode.EXTRA_SOURCE_PARAMS


# ---------------------------------------------------------------------------
# Factory: missing_params_fields
# ---------------------------------------------------------------------------


def test_missing_params_fields_code() -> None:
    err = ETLCompilationError.missing_params_fields(
        _Component,
        frozenset({"run_date"}),
        _Context,  # type: ignore[arg-type]
    )
    assert err.code is ETLErrorCode.MISSING_PARAMS_FIELDS


def test_missing_params_fields_component() -> None:
    err = ETLCompilationError.missing_params_fields(
        _Component,
        frozenset({"run_date"}),
        _Context,  # type: ignore[arg-type]
    )
    assert err.component == "MyComponent"


def test_missing_params_fields_message_contains_field() -> None:
    err = ETLCompilationError.missing_params_fields(
        _Component,
        frozenset({"run_date"}),
        _Context,  # type: ignore[arg-type]
    )
    assert "run_date" in str(err)


# ---------------------------------------------------------------------------
# Factory: unknown_source_table
# ---------------------------------------------------------------------------


def test_unknown_source_table_code() -> None:
    err = ETLCompilationError.unknown_source_table(_Step, "orders", "raw.orders")  # type: ignore[arg-type]
    assert err.code is ETLErrorCode.UNKNOWN_SOURCE_TABLE


def test_unknown_source_table_field_is_alias() -> None:
    err = ETLCompilationError.unknown_source_table(_Step, "orders", "raw.orders")  # type: ignore[arg-type]
    assert err.field == "orders"


def test_unknown_source_table_message() -> None:
    err = ETLCompilationError.unknown_source_table(_Step, "orders", "raw.orders")  # type: ignore[arg-type]
    assert "raw.orders" in str(err)
    assert "orders" in str(err)


# ---------------------------------------------------------------------------
# Factory: unknown_target_table
# ---------------------------------------------------------------------------


def test_unknown_target_table_code() -> None:
    err = ETLCompilationError.unknown_target_table(_Step, "staging.out")  # type: ignore[arg-type]
    assert err.code is ETLErrorCode.UNKNOWN_TARGET_TABLE


def test_unknown_target_table_field_is_none() -> None:
    err = ETLCompilationError.unknown_target_table(_Step, "staging.out")  # type: ignore[arg-type]
    assert err.field is None


# ---------------------------------------------------------------------------
# Factory: temp_not_produced
# ---------------------------------------------------------------------------


def test_temp_not_produced_code() -> None:
    err = ETLCompilationError.temp_not_produced(_Step, "tmp_orders", "orders_temp")  # type: ignore[arg-type]
    assert err.code is ETLErrorCode.TEMP_NOT_PRODUCED


def test_temp_not_produced_field_is_alias() -> None:
    err = ETLCompilationError.temp_not_produced(_Step, "tmp_orders", "orders_temp")  # type: ignore[arg-type]
    assert err.field == "tmp_orders"


def test_temp_not_produced_message_contains_temp_name() -> None:
    err = ETLCompilationError.temp_not_produced(_Step, "tmp_orders", "orders_temp")  # type: ignore[arg-type]
    assert "orders_temp" in str(err)


# ---------------------------------------------------------------------------
# Factory: upsert_no_keys
# ---------------------------------------------------------------------------


def test_upsert_no_keys_code() -> None:
    err = ETLCompilationError.upsert_no_keys(_Step)  # type: ignore[arg-type]
    assert err.code is ETLErrorCode.UPSERT_NO_KEYS


def test_upsert_no_keys_field_is_none() -> None:
    err = ETLCompilationError.upsert_no_keys(_Step)  # type: ignore[arg-type]
    assert err.field is None


# ---------------------------------------------------------------------------
# Factory: upsert_exclude_include_conflict
# ---------------------------------------------------------------------------


def test_upsert_exclude_include_conflict_code() -> None:
    err = ETLCompilationError.upsert_exclude_include_conflict(_Step)  # type: ignore[arg-type]
    assert err.code is ETLErrorCode.UPSERT_EXCLUDE_INCLUDE_CONFLICT


# ---------------------------------------------------------------------------
# Factory: upsert_key_in_exclude
# ---------------------------------------------------------------------------


def test_upsert_key_in_exclude_code() -> None:
    err = ETLCompilationError.upsert_key_in_exclude(_Step, frozenset({"id"}))  # type: ignore[arg-type]
    assert err.code is ETLErrorCode.UPSERT_KEY_IN_EXCLUDE


def test_upsert_key_in_exclude_message_contains_overlap() -> None:
    err = ETLCompilationError.upsert_key_in_exclude(_Step, frozenset({"id"}))  # type: ignore[arg-type]
    assert "id" in str(err)


# ---------------------------------------------------------------------------
# ETLCompilationError is still a plain Exception subclass
# ---------------------------------------------------------------------------


def test_is_exception() -> None:
    err = ETLCompilationError.upsert_no_keys(_Step)  # type: ignore[arg-type]
    assert isinstance(err, Exception)


def test_str_is_message() -> None:
    err = ETLCompilationError.upsert_no_keys(_Step)  # type: ignore[arg-type]
    assert str(err) == err.args[0]
