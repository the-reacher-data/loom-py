"""Tests for ETLCompiler catalog validation (sprint 3)."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from loom.etl import ETLParams, ETLStep, Format, FromFile, FromTable, IntoFile, IntoTable
from loom.etl.compiler import ETLCompilationError, ETLCompiler
from loom.etl.testing import StubCatalog


class RunParams(ETLParams):
    run_date: date


# ---------------------------------------------------------------------------
# Step declarations for the tests
# ---------------------------------------------------------------------------


class OrdersStep(ETLStep[RunParams]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.orders").replace()

    def execute(self, params: RunParams, *, orders: Any) -> Any:
        return orders


class MultiSourceStep(ETLStep[RunParams]):
    orders = FromTable("raw.orders")
    customers = FromTable("raw.customers")
    target = IntoTable("staging.out").replace()

    def execute(self, params: RunParams, *, orders: Any, customers: Any) -> Any:
        return orders


class FileSourceStep(ETLStep[RunParams]):
    report = FromFile("s3://raw/report.csv", format=Format.CSV)
    target = IntoTable("staging.report").replace()

    def execute(self, params: RunParams, *, report: Any) -> Any:
        return report


class FileTargetStep(ETLStep[RunParams]):
    orders = FromTable("raw.orders")
    target = IntoFile("s3://exports/out.csv", format=Format.CSV)

    def execute(self, params: RunParams, *, orders: Any) -> Any:
        return orders


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


def test_compile_without_catalog_skips_table_validation() -> None:
    plan = ETLCompiler().compile_step(OrdersStep)
    assert plan.step_type is OrdersStep


def test_compile_with_valid_catalog_succeeds() -> None:
    catalog = StubCatalog({"raw.orders": ("id",), "staging.orders": ()})
    plan = ETLCompiler(catalog=catalog).compile_step(OrdersStep)
    assert plan.step_type is OrdersStep


def test_compile_multi_source_all_registered() -> None:
    catalog = StubCatalog(
        {
            "raw.orders": ("id",),
            "raw.customers": ("id",),
            "staging.out": (),
        }
    )
    plan = ETLCompiler(catalog=catalog).compile_step(MultiSourceStep)
    assert len(plan.source_bindings) == 2


# ---------------------------------------------------------------------------
# FILE sources are not validated against the catalog
# ---------------------------------------------------------------------------


def test_file_source_not_validated_against_catalog() -> None:
    # Catalog has the target but NOT the file path — files are physical, not catalogued.
    catalog = StubCatalog({"staging.report": ()})
    plan = ETLCompiler(catalog=catalog).compile_step(FileSourceStep)
    assert plan.step_type is FileSourceStep


def test_file_target_not_validated_against_catalog() -> None:
    # Catalog has the TABLE source but not the file target — file targets have no TableRef.
    catalog = StubCatalog({"raw.orders": ("id",)})
    plan = ETLCompiler(catalog=catalog).compile_step(FileTargetStep)
    assert plan.step_type is FileTargetStep


# ---------------------------------------------------------------------------
# Catalog validation failures
# ---------------------------------------------------------------------------


def test_missing_source_table_raises() -> None:
    catalog = StubCatalog({"staging.orders": ()})  # raw.orders missing
    with pytest.raises(ETLCompilationError, match="unknown table 'raw.orders'"):
        ETLCompiler(catalog=catalog).compile_step(OrdersStep)


def test_missing_target_table_raises() -> None:
    catalog = StubCatalog({"raw.orders": ("id",)})  # staging.orders missing
    with pytest.raises(ETLCompilationError, match="unknown table 'staging.orders'"):
        ETLCompiler(catalog=catalog).compile_step(OrdersStep)


def test_missing_one_of_multiple_sources_raises() -> None:
    catalog = StubCatalog(
        {
            "raw.orders": ("id",),
            # raw.customers intentionally missing
            "staging.out": (),
        }
    )
    with pytest.raises(ETLCompilationError, match="unknown table 'raw.customers'"):
        ETLCompiler(catalog=catalog).compile_step(MultiSourceStep)


def test_error_message_includes_step_name() -> None:
    catalog = StubCatalog({"staging.orders": ()})
    with pytest.raises(ETLCompilationError, match="OrdersStep"):
        ETLCompiler(catalog=catalog).compile_step(OrdersStep)


# ---------------------------------------------------------------------------
# Compiler is reusable across steps with same catalog
# ---------------------------------------------------------------------------


def test_compiler_reuses_catalog_across_compile_calls() -> None:
    catalog = StubCatalog(
        {
            "raw.orders": ("id",),
            "staging.orders": (),
            "raw.customers": ("id",),
            "staging.out": (),
        }
    )
    compiler = ETLCompiler(catalog=catalog)
    plan1 = compiler.compile_step(OrdersStep)
    plan2 = compiler.compile_step(MultiSourceStep)
    assert plan1.step_type is OrdersStep
    assert plan2.step_type is MultiSourceStep
