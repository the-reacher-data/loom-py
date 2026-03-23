"""Tests for ETLStep declaration and __init_subclass__ validation."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from loom.etl import (
    ETLParams,
    ETLStep,
    FromTable,
    IntoTable,
    Sources,
    SourceSet,
)
from loom.etl._step import _SourceForm


class RunParams(ETLParams):
    run_date: date
    countries: tuple[str, ...]


# ---------------------------------------------------------------------------
# Valid step declarations
# ---------------------------------------------------------------------------


class StepForm2(ETLStep[RunParams]):
    sources = Sources(
        orders=FromTable("raw.orders"),
        customers=FromTable("raw.customers"),
    )
    target = IntoTable("staging.out").replace()

    def execute(self, params: RunParams, *, orders: Any, customers: Any) -> Any:
        return orders


class StepForm1(ETLStep[RunParams]):
    orders = FromTable("raw.orders")
    customers = FromTable("raw.customers")
    target = IntoTable("staging.out").replace()

    def execute(self, params: RunParams, *, orders: Any, customers: Any) -> Any:
        return orders


class StepNoSources(ETLStep[RunParams]):
    target = IntoTable("staging.calendar").replace()

    def execute(self, params: RunParams) -> Any:
        return None


class OrderSources(SourceSet[RunParams]):
    orders = FromTable("raw.orders")


class StepForm3(ETLStep[RunParams]):
    sources = OrderSources.extended(customers=FromTable("raw.customers"))
    target = IntoTable("staging.out").replace()

    def execute(self, params: RunParams, *, orders: Any, customers: Any) -> Any:
        return orders


# ---------------------------------------------------------------------------
# Tests: source form detection
# ---------------------------------------------------------------------------


def test_form2_detected_as_grouped() -> None:
    assert StepForm2._source_form is _SourceForm.GROUPED


def test_form1_detected_as_inline() -> None:
    assert StepForm1._source_form is _SourceForm.INLINE


def test_form1_inline_sources_collected() -> None:
    assert set(StepForm1._inline_sources) == {"orders", "customers"}


def test_form3_detected_as_grouped() -> None:
    assert StepForm3._source_form is _SourceForm.GROUPED


def test_no_sources_form_is_none() -> None:
    assert StepNoSources._source_form is _SourceForm.NONE


# ---------------------------------------------------------------------------
# Tests: params type extraction
# ---------------------------------------------------------------------------


def test_params_type_extracted() -> None:
    assert StepForm2._params_type is RunParams


def test_params_type_none_for_base_class() -> None:
    assert ETLStep._params_type is None


# ---------------------------------------------------------------------------
# Tests: mixing forms raises TypeError
# ---------------------------------------------------------------------------


def test_mixing_inline_and_grouped_raises() -> None:
    with pytest.raises(TypeError, match="cannot mix inline source attributes"):

        class _BadStep(ETLStep[RunParams]):
            orders = FromTable("raw.orders")
            sources = Sources(customers=FromTable("raw.customers"))
            target = IntoTable("staging.out").replace()

            def execute(self, params: RunParams, *, orders: Any, customers: Any) -> Any:
                return orders


# ---------------------------------------------------------------------------
# Tests: execute() raises NotImplementedError on base
# ---------------------------------------------------------------------------


def test_execute_raises_not_implemented_on_base() -> None:
    class Bare(ETLStep[RunParams]):
        target = IntoTable("staging.x").replace()

    with pytest.raises(NotImplementedError):
        Bare().execute(RunParams(run_date=date(2024, 1, 1), countries=()))
