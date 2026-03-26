"""Unit tests for compiler._temp_validator — direct function coverage."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from loom.etl import (
    ETLParams,
    ETLPipeline,
    ETLProcess,
    ETLStep,
    FromTable,
    FromTemp,
    IntoTable,
    IntoTemp,
)
from loom.etl.compiler import ETLCompilationError, ETLCompiler
from loom.etl.compiler._temp_validator import validate_plan_temps


class P(ETLParams):
    run_date: date


class ProduceStep(ETLStep[P]):
    orders = FromTable("raw.orders")
    target = IntoTemp("orders_temp")

    def execute(self, params: P, *, orders: Any) -> Any:
        return orders


class ConsumeStep(ETLStep[P]):
    orders_temp = FromTemp("orders_temp")
    target = IntoTable("staging.out").replace()

    def execute(self, params: P, *, orders_temp: Any) -> Any:
        return orders_temp


class ValidProc(ETLProcess[P]):
    steps = [ProduceStep, ConsumeStep]


class ValidPipeline(ETLPipeline[P]):
    processes = [ValidProc]


# ---------------------------------------------------------------------------
# Valid temp flow passes
# ---------------------------------------------------------------------------


def test_valid_temp_flow_passes() -> None:
    plan = ETLCompiler().compile(ValidPipeline)
    validate_plan_temps(plan)


# ---------------------------------------------------------------------------
# Forward reference (consume before produce) raises
# ---------------------------------------------------------------------------


class ConsumeFirstProc(ETLProcess[P]):
    steps = [ConsumeStep, ProduceStep]


class ConsumeFirstPipeline(ETLPipeline[P]):
    processes = [ConsumeFirstProc]


def test_forward_temp_reference_raises() -> None:
    with pytest.raises(ETLCompilationError, match="orders_temp"):
        ETLCompiler().compile(ConsumeFirstPipeline)


# ---------------------------------------------------------------------------
# Orphan temp (no IntoTemp anywhere) raises
# ---------------------------------------------------------------------------


class OrphanConsumeProc(ETLProcess[P]):
    steps = [ConsumeStep]


class OrphanConsumePipeline(ETLPipeline[P]):
    processes = [OrphanConsumeProc]


def test_orphan_temp_reference_raises() -> None:
    with pytest.raises(ETLCompilationError, match="orders_temp"):
        ETLCompiler().compile(OrphanConsumePipeline)
