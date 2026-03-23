"""Tests for ETLCompiler static validation."""

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
    IntoTable,
    Sources,
)
from loom.etl.compiler import (
    Backend,
    ETLCompilationError,
    ETLCompiler,
    ParallelProcessGroup,
    ParallelStepGroup,
)


class RunParams(ETLParams):
    run_date: date
    countries: tuple[str, ...]


class ExtractStep(ETLStep[RunParams]):
    sources = Sources(orders=FromTable("raw.orders"))
    target = IntoTable("staging.orders").replace()

    def execute(self, params: RunParams, *, orders: Any) -> Any:
        return orders


class EnrichAStep(ETLStep[RunParams]):
    sources = Sources(orders=FromTable("staging.orders"))
    target = IntoTable("staging.enriched_a").replace()

    def execute(self, params: RunParams, *, orders: Any) -> Any:
        return orders


class EnrichBStep(ETLStep[RunParams]):
    sources = Sources(orders=FromTable("staging.orders"))
    target = IntoTable("staging.enriched_b").replace()

    def execute(self, params: RunParams, *, orders: Any) -> Any:
        return orders


class AggStep(ETLStep[RunParams]):
    sources = Sources(
        enriched_a=FromTable("staging.enriched_a"),
        enriched_b=FromTable("staging.enriched_b"),
    )
    target = IntoTable("mart.summary").replace()

    def execute(self, params: RunParams, *, enriched_a: Any, enriched_b: Any) -> Any:
        return enriched_a


class NoSourceStep(ETLStep[RunParams]):
    target = IntoTable("staging.calendar").replace()

    def execute(self, params: RunParams) -> Any:
        return None


class StagingProcess(ETLProcess[RunParams]):
    steps = [ExtractStep, [EnrichAStep, EnrichBStep], AggStep]


class DailyPipeline(ETLPipeline[RunParams]):
    processes = [StagingProcess]


def test_compile_step_returns_step_plan() -> None:
    plan = ETLCompiler().compile_step(ExtractStep)
    assert plan.step_type is ExtractStep
    assert plan.params_type is RunParams


def test_compile_step_source_bindings() -> None:
    plan = ETLCompiler().compile_step(ExtractStep)
    assert len(plan.source_bindings) == 1
    assert plan.source_bindings[0].alias == "orders"


def test_compile_step_target_binding() -> None:
    plan = ETLCompiler().compile_step(ExtractStep)
    assert plan.target_binding.spec.table_ref.ref == "staging.orders"


def test_compile_step_multiple_sources() -> None:
    plan = ETLCompiler().compile_step(AggStep)
    aliases = {b.alias for b in plan.source_bindings}
    assert aliases == {"enriched_a", "enriched_b"}


def test_compile_step_no_sources() -> None:
    plan = ETLCompiler().compile_step(NoSourceStep)
    assert plan.source_bindings == ()


def test_compile_step_is_cached() -> None:
    compiler = ETLCompiler()
    p1 = compiler.compile_step(ExtractStep)
    p2 = compiler.compile_step(ExtractStep)
    assert p1 is p2


def test_backend_unknown_when_no_type_hints() -> None:
    class UntypedStep(ETLStep[RunParams]):
        target = IntoTable("staging.x").replace()

        def execute(self, params, *, orders):  # type: ignore[override]
            return orders

        sources = Sources(orders=FromTable("raw.x"))

    plan = ETLCompiler().compile_step(UntypedStep)
    assert plan.backend is Backend.UNKNOWN


def test_missing_target_raises() -> None:
    class NoTarget(ETLStep[RunParams]):
        sources = Sources(orders=FromTable("raw.orders"))

        def execute(self, params: RunParams, *, orders: Any) -> Any:
            return orders

    with pytest.raises(ETLCompilationError, match="target.*required"):
        ETLCompiler().compile_step(NoTarget)


def test_source_missing_from_execute_raises() -> None:
    class MissingFrame(ETLStep[RunParams]):
        sources = Sources(
            orders=FromTable("raw.orders"),
            customers=FromTable("raw.customers"),
        )
        target = IntoTable("staging.x").replace()

        def execute(self, params: RunParams, *, orders: Any) -> Any:
            return orders  # customers missing

    with pytest.raises(ETLCompilationError, match="customers"):
        ETLCompiler().compile_step(MissingFrame)


def test_extra_execute_param_not_in_sources_raises() -> None:
    class ExtraFrame(ETLStep[RunParams]):
        sources = Sources(orders=FromTable("raw.orders"))
        target = IntoTable("staging.x").replace()

        def execute(self, params: RunParams, *, orders: Any, ghost: Any) -> Any:
            return orders  # ghost not in sources

    with pytest.raises(ETLCompilationError, match="ghost"):
        ETLCompiler().compile_step(ExtraFrame)


def test_missing_params_arg_raises() -> None:
    class NoParams(ETLStep[RunParams]):
        target = IntoTable("staging.x").replace()

        def execute(self) -> Any:  # type: ignore[override]
            return None

    with pytest.raises(ETLCompilationError, match="params"):
        ETLCompiler().compile_step(NoParams)


def test_bare_step_without_generic_raises() -> None:
    class BareStep(ETLStep):  # type: ignore[type-arg]
        target = IntoTable("staging.x").replace()

        def execute(self, params: Any) -> Any:
            return None

    with pytest.raises(ETLCompilationError, match="missing generic parameter"):
        ETLCompiler().compile_step(BareStep)


def test_compile_process_sequential_and_parallel() -> None:
    plan = ETLCompiler().compile_process(StagingProcess)
    assert plan.process_type is StagingProcess
    # nodes: ExtractStep, [EnrichAStep, EnrichBStep], AggStep
    assert len(plan.nodes) == 3
    assert isinstance(plan.nodes[1], ParallelStepGroup)
    assert len(plan.nodes[1].plans) == 2


def test_compile_process_invalid_item_raises() -> None:
    class BadProcess(ETLProcess[RunParams]):
        steps = ["not_a_step"]  # type: ignore[list-item]

    with pytest.raises(ETLCompilationError, match="ETLStep subclass"):
        ETLCompiler().compile_process(BadProcess)


def test_compile_pipeline_returns_pipeline_plan() -> None:
    plan = ETLCompiler().compile(DailyPipeline)
    assert plan.pipeline_type is DailyPipeline
    assert plan.params_type is RunParams
    assert len(plan.nodes) == 1


def test_compile_pipeline_parallel_processes() -> None:
    class ParallelPipeline(ETLPipeline[RunParams]):
        processes = [[StagingProcess, StagingProcess]]

    plan = ETLCompiler().compile(ParallelPipeline)
    assert len(plan.nodes) == 1
    assert isinstance(plan.nodes[0], ParallelProcessGroup)


def test_compile_pipeline_invalid_process_raises() -> None:
    class BadPipeline(ETLPipeline[RunParams]):
        processes = ["not_a_process"]  # type: ignore[list-item]

    with pytest.raises(ETLCompilationError, match="ETLProcess subclass"):
        ETLCompiler().compile(BadPipeline)
