"""Compiler-time validation tests for UPSERT target specs."""

from __future__ import annotations

from typing import Any

import pytest

from loom.etl import ETLParams, ETLStep, IntoTable
from loom.etl.compiler import ETLCompilationError, ETLCompiler


class _Params(ETLParams):
    pass


def _compiler() -> ETLCompiler:
    return ETLCompiler()


# ---------------------------------------------------------------------------
# Valid UPSERT specs compile without errors
# ---------------------------------------------------------------------------


class _ValidUpsert(ETLStep[_Params]):
    target = IntoTable("test.orders").upsert(keys=("order_id",))

    def execute(self, params: _Params) -> Any:
        return None


def test_valid_upsert_compiles() -> None:
    plan = _compiler().compile_step(_ValidUpsert)
    assert plan.target_binding.spec.upsert_keys == ("order_id",)


class _UpsertWithPartitions(ETLStep[_Params]):
    target = IntoTable("test.orders").upsert(
        keys=("order_id",),
        partition_cols=("year", "month"),
    )

    def execute(self, params: _Params) -> Any:
        return None


def test_valid_upsert_with_partition_cols_compiles() -> None:
    plan = _compiler().compile_step(_UpsertWithPartitions)
    spec = plan.target_binding.spec
    assert spec.upsert_keys == ("order_id",)
    assert spec.partition_cols == ("year", "month")


class _UpsertWithExclude(ETLStep[_Params]):
    target = IntoTable("test.orders").upsert(
        keys=("order_id",),
        exclude=("created_at",),
    )

    def execute(self, params: _Params) -> Any:
        return None


def test_valid_upsert_with_exclude_compiles() -> None:
    plan = _compiler().compile_step(_UpsertWithExclude)
    assert plan.target_binding.spec.upsert_exclude == ("created_at",)


class _UpsertWithInclude(ETLStep[_Params]):
    target = IntoTable("test.orders").upsert(
        keys=("order_id",),
        include=("status", "updated_at"),
    )

    def execute(self, params: _Params) -> Any:
        return None


def test_valid_upsert_with_include_compiles() -> None:
    plan = _compiler().compile_step(_UpsertWithInclude)
    assert plan.target_binding.spec.upsert_include == ("status", "updated_at")


# ---------------------------------------------------------------------------
# Empty keys raises ETLCompilationError
# ---------------------------------------------------------------------------


class _EmptyKeys(ETLStep[_Params]):
    target = IntoTable("test.orders").upsert(keys=())

    def execute(self, params: _Params) -> Any:
        return None


def test_upsert_empty_keys_raises() -> None:
    with pytest.raises(ETLCompilationError, match="requires at least one key"):
        _compiler().compile_step(_EmptyKeys)


# ---------------------------------------------------------------------------
# exclude and include are mutually exclusive
# ---------------------------------------------------------------------------


class _ExcludeAndInclude(ETLStep[_Params]):
    target = IntoTable("test.orders").upsert(
        keys=("id",),
        exclude=("created_at",),
        include=("status",),
    )

    def execute(self, params: _Params) -> Any:
        return None


def test_upsert_exclude_and_include_raises() -> None:
    with pytest.raises(ETLCompilationError, match="mutually exclusive"):
        _compiler().compile_step(_ExcludeAndInclude)


# ---------------------------------------------------------------------------
# exclude must not overlap with upsert_keys
# ---------------------------------------------------------------------------


class _ExcludeOverlapsKeys(ETLStep[_Params]):
    target = IntoTable("test.orders").upsert(
        keys=("order_id",),
        exclude=("order_id", "created_at"),
    )

    def execute(self, params: _Params) -> Any:
        return None


def test_upsert_exclude_overlaps_keys_raises() -> None:
    with pytest.raises(ETLCompilationError, match="overlaps with upsert keys"):
        _compiler().compile_step(_ExcludeOverlapsKeys)
