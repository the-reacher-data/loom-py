"""Runtime-executed coverage for all ETLCompilationError factory methods."""

from __future__ import annotations

from typing import Any

import pytest

from loom.etl.compiler._errors import ETLCompilationError, ETLErrorCode


class _Step:
    __qualname__ = "MyStep"


class _Process:
    __qualname__ = "MyProcess"


class _Pipeline:
    __qualname__ = "MyPipeline"


class _Component:
    __qualname__ = "MyComponent"


class _ExpectedParams:
    __name__ = "ExpectedParams"


class _ContextParams:
    __name__ = "ContextParams"


@pytest.mark.parametrize(
    ("factory", "code", "field"),
    [
        (
            lambda: ETLCompilationError.invalid_params_type(_Step, _ExpectedParams),  # type: ignore[arg-type]
            ETLErrorCode.INVALID_PARAMS_TYPE,
            "params",
        ),
        (
            lambda: ETLCompilationError.invalid_params_name(_Step, "ctx"),  # type: ignore[arg-type]
            ETLErrorCode.INVALID_PARAMS_NAME,
            "ctx",
        ),
        (
            lambda: ETLCompilationError.missing_source_params(_Step, frozenset({"orders"})),  # type: ignore[arg-type]
            ETLErrorCode.MISSING_SOURCE_PARAMS,
            None,
        ),
        (
            lambda: ETLCompilationError.extra_source_params(_Step, frozenset({"ghost"})),  # type: ignore[arg-type]
            ETLErrorCode.EXTRA_SOURCE_PARAMS,
            None,
        ),
        (
            lambda: ETLCompilationError.missing_params_fields(  # type: ignore[arg-type]
                _Component, frozenset({"run_date"}), _ContextParams
            ),
            ETLErrorCode.MISSING_PARAMS_FIELDS,
            None,
        ),
        (
            lambda: ETLCompilationError.unknown_source_table(  # type: ignore[arg-type]
                _Step, "orders", "raw.orders"
            ),
            ETLErrorCode.UNKNOWN_SOURCE_TABLE,
            "orders",
        ),
        (
            lambda: ETLCompilationError.unknown_target_table(_Step, "staging.orders"),  # type: ignore[arg-type]
            ETLErrorCode.UNKNOWN_TARGET_TABLE,
            None,
        ),
        (
            lambda: ETLCompilationError.temp_not_produced(_Step, "tmp", "tmp_orders"),  # type: ignore[arg-type]
            ETLErrorCode.TEMP_NOT_PRODUCED,
            "tmp",
        ),
        (
            lambda: ETLCompilationError.upsert_no_keys(_Step),  # type: ignore[arg-type]
            ETLErrorCode.UPSERT_NO_KEYS,
            None,
        ),
        (
            lambda: ETLCompilationError.upsert_exclude_include_conflict(_Step),  # type: ignore[arg-type]
            ETLErrorCode.UPSERT_EXCLUDE_INCLUDE_CONFLICT,
            None,
        ),
        (
            lambda: ETLCompilationError.upsert_key_in_exclude(  # type: ignore[arg-type]
                _Step, frozenset({"id"})
            ),
            ETLErrorCode.UPSERT_KEY_IN_EXCLUDE,
            None,
        ),
        (
            lambda: ETLCompilationError.missing_generic_param(_Step, "ETLStep"),  # type: ignore[arg-type]
            ETLErrorCode.MISSING_GENERIC_PARAM,
            None,
        ),
        (
            lambda: ETLCompilationError.missing_target(_Step),  # type: ignore[arg-type]
            ETLErrorCode.MISSING_TARGET,
            None,
        ),
        (
            lambda: ETLCompilationError.invalid_target_type(_Step),  # type: ignore[arg-type]
            ETLErrorCode.INVALID_TARGET_TYPE,
            None,
        ),
        (
            lambda: ETLCompilationError.invalid_sources_type(_Step),  # type: ignore[arg-type]
            ETLErrorCode.INVALID_SOURCES_TYPE,
            None,
        ),
        (
            lambda: ETLCompilationError.invalid_process_item(_Pipeline, object()),  # type: ignore[arg-type]
            ETLErrorCode.INVALID_PROCESS_ITEM,
            None,
        ),
        (
            lambda: ETLCompilationError.invalid_step_item(_Process, object()),  # type: ignore[arg-type]
            ETLErrorCode.INVALID_STEP_ITEM,
            None,
        ),
        (
            lambda: ETLCompilationError.duplicate_temp_name(_Step, "tmp"),  # type: ignore[arg-type]
            ETLErrorCode.DUPLICATE_TEMP_NAME,
            "tmp",
        ),
        (
            lambda: ETLCompilationError.invalid_temp_append_mix(_Step, "tmp"),  # type: ignore[arg-type]
            ETLErrorCode.INVALID_TEMP_APPEND_MIX,
            "tmp",
        ),
        (
            lambda: ETLCompilationError.unknown_param_field(  # type: ignore[arg-type]
                _Step, "missing", _ContextParams
            ),
            ETLErrorCode.UNKNOWN_PARAM_FIELD,
            "missing",
        ),
    ],
)
def test_all_factory_methods_runtime_execution(
    factory: Any,
    code: ETLErrorCode,
    field: str | None,
) -> None:
    err = factory()
    assert isinstance(err, ETLCompilationError)
    assert err.code == code
    assert err.field == field
    assert err.component
    assert str(err)
