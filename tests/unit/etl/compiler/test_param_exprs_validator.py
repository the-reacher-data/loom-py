"""Unit tests for compiler.validators._param_exprs — ParamExpr field validation."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from loom.etl import ETLParams, ETLStep, IntoTable, col
from loom.etl.compiler import ETLCompilationError
from loom.etl.compiler._errors import ETLErrorCode
from loom.etl.compiler._plan import SourceBinding, TargetBinding
from loom.etl.compiler.validators._param_exprs import validate_param_exprs
from loom.etl.io.source import TableSourceSpec
from loom.etl.io.target._table import AppendSpec, ReplaceWhereSpec
from loom.etl.pipeline._proxy import params as p
from loom.etl.schema._table import TableRef


class _P(ETLParams):
    run_date: date
    country: str


def _append_target() -> TargetBinding:
    return TargetBinding(spec=AppendSpec(table_ref=TableRef("t.out")))


def _replace_where_target(pred: Any) -> TargetBinding:
    return TargetBinding(spec=ReplaceWhereSpec(table_ref=TableRef("t.out"), replace_predicate=pred))


def _source_binding(*predicates: Any) -> SourceBinding:
    return SourceBinding(
        alias="data",
        spec=TableSourceSpec(
            alias="data",
            table_ref=TableRef("raw.data"),
            predicates=predicates,
        ),
    )


class _Step(ETLStep[_P]):
    target = IntoTable("t.out").append()

    def execute(self, params: _P) -> Any:
        return None


# ---------------------------------------------------------------------------
# No predicates — always valid
# ---------------------------------------------------------------------------


def test_no_predicates_passes() -> None:
    validate_param_exprs(_Step, _P, (), _append_target())


# ---------------------------------------------------------------------------
# Valid field references
# ---------------------------------------------------------------------------


def test_valid_source_predicate_passes() -> None:
    pred = col("year") == p.run_date.year  # run_date is a declared field
    validate_param_exprs(_Step, _P, (_source_binding(pred),), _append_target())


def test_valid_replace_where_predicate_passes() -> None:
    pred = col("country") == p.country  # country is a declared field
    validate_param_exprs(_Step, _P, (), _replace_where_target(pred))


def test_nested_attribute_on_valid_field_passes() -> None:
    # params.run_date.year — run_date is valid; .year is datetime.date attr, not validated
    pred = col("year") == p.run_date.year
    validate_param_exprs(_Step, _P, (_source_binding(pred),), _append_target())


# ---------------------------------------------------------------------------
# Unknown field references raise ETLCompilationError
# ---------------------------------------------------------------------------


def test_unknown_field_in_source_predicate_raises() -> None:
    pred = col("year") == p.bad_field
    with pytest.raises(ETLCompilationError) as exc_info:
        validate_param_exprs(_Step, _P, (_source_binding(pred),), _append_target())
    assert exc_info.value.code == ETLErrorCode.UNKNOWN_PARAM_FIELD
    assert exc_info.value.field == "bad_field"


def test_unknown_field_in_replace_where_raises() -> None:
    pred = col("region") == p.unknown_region
    with pytest.raises(ETLCompilationError) as exc_info:
        validate_param_exprs(_Step, _P, (), _replace_where_target(pred))
    assert exc_info.value.code == ETLErrorCode.UNKNOWN_PARAM_FIELD
    assert exc_info.value.field == "unknown_region"


# ---------------------------------------------------------------------------
# Compound predicates (AND / OR / NOT / IN) are traversed recursively
# ---------------------------------------------------------------------------


def test_and_predicate_both_sides_traversed() -> None:
    pred = (col("year") == p.run_date.year) & (col("country") == p.bad_field)
    with pytest.raises(ETLCompilationError) as exc_info:
        validate_param_exprs(_Step, _P, (_source_binding(pred),), _append_target())
    assert exc_info.value.field == "bad_field"


def test_or_predicate_traversed() -> None:
    pred = (col("a") == p.run_date) | (col("b") == p.nope)
    with pytest.raises(ETLCompilationError):
        validate_param_exprs(_Step, _P, (_source_binding(pred),), _append_target())


def test_not_predicate_traversed() -> None:
    pred = ~(col("a") == p.missing)
    with pytest.raises(ETLCompilationError):
        validate_param_exprs(_Step, _P, (_source_binding(pred),), _append_target())


def test_in_predicate_ref_traversed() -> None:
    pred = col("country").isin(p.country)  # type: ignore[arg-type]
    validate_param_exprs(_Step, _P, (_source_binding(pred),), _append_target())


def test_in_predicate_unknown_ref_raises() -> None:
    pred = col("x").isin(p.bad)  # type: ignore[arg-type]
    with pytest.raises(ETLCompilationError) as exc_info:
        validate_param_exprs(_Step, _P, (_source_binding(pred),), _append_target())
    assert exc_info.value.field == "bad"


# ---------------------------------------------------------------------------
# Non-msgspec params type is skipped gracefully
# ---------------------------------------------------------------------------


class _PlainParams:
    run_date: date


def test_non_struct_params_skipped() -> None:
    pred = col("year") == p.anything_goes
    validate_param_exprs(_Step, _PlainParams, (_source_binding(pred),), _append_target())
