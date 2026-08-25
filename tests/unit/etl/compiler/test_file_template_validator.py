"""Unit tests for compile-time validation of file path templates."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from loom.etl import ETLParams, ETLStep, IntoTable
from loom.etl.compiler import ETLCompilationError
from loom.etl.compiler._errors import ETLErrorCode
from loom.etl.compiler._plan import SourceBinding, TargetBinding
from loom.etl.compiler._validators_step import validate_file_path_templates
from loom.etl.declarative._format import Format
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.source import FileSourceSpec
from loom.etl.declarative.target._file import FileSpec
from loom.etl.declarative.target._table import AppendSpec


class _P(ETLParams):
    run_date: date


class _Step(ETLStep[_P]):
    target = IntoTable("t.out").append()

    def execute(self, params: _P) -> Any:
        return None


def _file_source(path: str, *, is_alias: bool = False) -> SourceBinding:
    return SourceBinding(
        alias="data",
        spec=FileSourceSpec(alias="data", path=path, format=Format.CSV, is_alias=is_alias),
    )


def _table_target() -> TargetBinding:
    return TargetBinding(spec=AppendSpec(table_ref=TableRef("t.out")))


def test_known_field_in_source_path_passes() -> None:
    validate_file_path_templates(
        _Step, _P, (_file_source("s3://raw/{run_date:%Y%m%d}.csv"),), _table_target()
    )


def test_unknown_field_in_source_path_raises() -> None:
    with pytest.raises(ETLCompilationError) as excinfo:
        validate_file_path_templates(
            _Step, _P, (_file_source("s3://raw/{nope}.csv"),), _table_target()
        )
    assert excinfo.value.code is ETLErrorCode.UNKNOWN_TEMPLATE_FIELD
    assert excinfo.value.field == "nope"


def test_unknown_field_in_target_path_raises() -> None:
    target = TargetBinding(spec=FileSpec(path="s3://out/{nope}.csv", format=Format.CSV))
    with pytest.raises(ETLCompilationError) as excinfo:
        validate_file_path_templates(_Step, _P, (), target)
    assert excinfo.value.code is ETLErrorCode.UNKNOWN_TEMPLATE_FIELD


def test_known_field_in_target_path_passes() -> None:
    target = TargetBinding(spec=FileSpec(path="s3://out/{run_date}.csv", format=Format.CSV))
    validate_file_path_templates(_Step, _P, (), target)


def test_alias_paths_are_not_validated_at_compile_time() -> None:
    """Alias URIs live in storage config; template check happens at runtime."""
    target = TargetBinding(spec=FileSpec(path="exports_daily", format=Format.CSV, is_alias=True))
    validate_file_path_templates(_Step, _P, (_file_source("events_raw", is_alias=True),), target)


# ---------------------------------------------------------------------------
# Read-only properties on the params class are known template fields
# ---------------------------------------------------------------------------


class _PWithProperty(ETLParams):
    run_date: date

    @property
    def partition_day(self) -> date:
        return self.run_date


def test_property_in_source_path_passes() -> None:
    validate_file_path_templates(
        _Step, _PWithProperty, (_file_source("s3://raw/{partition_day}.csv"),), _table_target()
    )


def test_unknown_field_still_raises_with_property_params() -> None:
    with pytest.raises(ETLCompilationError) as excinfo:
        validate_file_path_templates(
            _Step, _PWithProperty, (_file_source("s3://raw/{nope}.csv"),), _table_target()
        )
    assert excinfo.value.code is ETLErrorCode.UNKNOWN_TEMPLATE_FIELD
    assert excinfo.value.field == "nope"
