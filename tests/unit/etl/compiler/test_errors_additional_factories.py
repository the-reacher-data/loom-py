"""Additional coverage for ETLCompilationError factory methods."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from loom.etl.compiler._errors import ETLCompilationError, ETLErrorCode


class _Step:
    __qualname__ = "MyStep"


class _Process:
    __qualname__ = "MyProcess"


class _Pipeline:
    __qualname__ = "MyPipeline"


def test_missing_generic_param_factory() -> None:
    err = ETLCompilationError.missing_generic_param(_Step, "ETLStep")  # type: ignore[arg-type]
    assert err.code is ETLErrorCode.MISSING_GENERIC_PARAM
    assert err.component == "MyStep"
    assert "missing generic parameter" in str(err)


@pytest.mark.parametrize(
    "factory,code,field,contains",
    [
        (
            lambda: ETLCompilationError.missing_target(_Step),  # type: ignore[arg-type]
            ETLErrorCode.MISSING_TARGET,
            None,
            "'target' is required",
        ),
        (
            lambda: ETLCompilationError.invalid_target_type(_Step),  # type: ignore[arg-type]
            ETLErrorCode.INVALID_TARGET_TYPE,
            None,
            "IntoTable",
        ),
        (
            lambda: ETLCompilationError.invalid_sources_type(_Step),  # type: ignore[arg-type]
            ETLErrorCode.INVALID_SOURCES_TYPE,
            None,
            "Sources",
        ),
        (
            lambda: ETLCompilationError.invalid_process_item(_Pipeline, object()),  # type: ignore[arg-type]
            ETLErrorCode.INVALID_PROCESS_ITEM,
            None,
            "ETLProcess subclass",
        ),
        (
            lambda: ETLCompilationError.invalid_step_item(_Process, object()),  # type: ignore[arg-type]
            ETLErrorCode.INVALID_STEP_ITEM,
            None,
            "ETLStep subclass",
        ),
    ],
)
def test_factory_codes_and_messages(
    factory: Callable[[], ETLCompilationError],
    code: ETLErrorCode,
    field: str | None,
    contains: str,
) -> None:
    err = factory()
    assert isinstance(err, ETLCompilationError)
    assert err.code is code
    assert err.field == field
    assert contains in str(err)
