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
from loom.etl._target import SchemaMode
from loom.etl.compiler import (
    Backend,
    ETLCompilationError,
    ETLCompiler,
    ParallelProcessGroup,
    ParallelStepGroup,
)
from loom.etl.testing import StubCatalog


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


# ---------------------------------------------------------------------------
# Params contract validation
# ---------------------------------------------------------------------------


class NarrowParams(ETLParams):
    run_date: date  # subset of RunParams (no 'countries')


class NarrowStep(ETLStep[NarrowParams]):
    target = IntoTable("staging.narrow").replace()

    def execute(self, params: NarrowParams) -> Any:
        return None


class NarrowProcess(ETLProcess[NarrowParams]):
    steps = [NarrowStep]


class UnrelatedParams(ETLParams):
    region: str  # completely different fields


class UnrelatedStep(ETLStep[UnrelatedParams]):
    target = IntoTable("staging.unrelated").replace()

    def execute(self, params: UnrelatedParams) -> Any:
        return None


class UnrelatedProcess(ETLProcess[UnrelatedParams]):
    steps = [UnrelatedStep]


def test_params_compat_same_type_passes() -> None:
    """A step with the exact same params type as the process compiles fine."""
    plan = ETLCompiler().compile_process(StagingProcess)
    assert plan is not None


def test_params_compat_subset_in_process_passes() -> None:
    """Step with NarrowParams ⊂ RunParams compiles fine in a RunParams process."""

    class WideProcess(ETLProcess[RunParams]):
        steps = [NarrowStep]

    plan = ETLCompiler().compile_process(WideProcess)
    assert plan is not None


def test_params_compat_subset_in_pipeline_passes() -> None:
    """Process with NarrowParams ⊂ RunParams compiles fine in a RunParams pipeline."""

    class WidePipeline(ETLPipeline[RunParams]):
        processes = [NarrowProcess]

    plan = ETLCompiler().compile(WidePipeline)
    assert plan is not None


def test_params_compat_missing_field_in_process_raises() -> None:
    """Step requires 'region' which is absent from RunParams → error at compile."""

    class MixedProcess(ETLProcess[RunParams]):
        steps = [UnrelatedStep]

    with pytest.raises(ETLCompilationError, match="region"):
        ETLCompiler().compile_process(MixedProcess)


def test_params_compat_missing_field_in_pipeline_raises() -> None:
    """Process requires 'region' which is absent from RunParams → error at compile."""

    class MixedPipeline(ETLPipeline[RunParams]):
        processes = [UnrelatedProcess]

    with pytest.raises(ETLCompilationError, match="region"):
        ETLCompiler().compile(MixedPipeline)


def test_params_compat_error_names_component() -> None:
    """Error message includes the component class name."""

    class MixedProcess(ETLProcess[RunParams]):
        steps = [UnrelatedStep]

    with pytest.raises(ETLCompilationError, match="UnrelatedStep"):
        ETLCompiler().compile_process(MixedProcess)


def test_params_compat_parallel_group_validated() -> None:
    """Params compat is enforced even for steps inside a parallel group."""

    class ParallelMixedProcess(ETLProcess[RunParams]):
        steps = [[NarrowStep, UnrelatedStep]]

    with pytest.raises(ETLCompilationError, match="region"):
        ETLCompiler().compile_process(ParallelMixedProcess)


# ---------------------------------------------------------------------------
# Catalog validation — will_create forward tracking
# ---------------------------------------------------------------------------


class CreateStep(ETLStep[RunParams]):
    """Creates staging.new with OVERWRITE — table need not exist beforehand."""

    target = IntoTable("staging.new").replace(schema=SchemaMode.OVERWRITE)

    def execute(self, params: RunParams) -> Any:
        return None


class ConsumeStep(ETLStep[RunParams]):
    """Reads staging.new produced by CreateStep."""

    sources = Sources(data=FromTable("staging.new"))
    target = IntoTable("mart.out").replace()

    def execute(self, params: RunParams, *, data: Any) -> Any:
        return data


class CreateThenConsumePipeline(ETLPipeline[RunParams]):
    class _Proc(ETLProcess[RunParams]):
        steps = [CreateStep, ConsumeStep]

    processes = [_Proc]


def test_catalog_overwrite_target_does_not_require_existence() -> None:
    """OVERWRITE target compiles even if the table is absent from the catalog."""
    catalog = StubCatalog(tables={"raw.orders": (), "mart.out": ()})
    ETLCompiler(catalog).compile(CreateThenConsumePipeline)


def test_catalog_source_created_by_prior_step_passes() -> None:
    """Source referencing a table created by a prior OVERWRITE step passes.

    staging.new is NOT in the catalog — CreateStep creates it (OVERWRITE),
    so ConsumeStep can reference it as a source without a catalog hit.
    """
    catalog = StubCatalog(tables={"mart.out": ()})
    ETLCompiler(catalog).compile(CreateThenConsumePipeline)


def test_catalog_source_genuinely_missing_raises() -> None:
    """Source for a table that nobody creates raises ETLCompilationError."""

    class MissingSourcePipeline(ETLPipeline[RunParams]):
        class _Proc(ETLProcess[RunParams]):
            steps = [ConsumeStep]

        processes = [_Proc]

    catalog = StubCatalog(tables={"mart.out": ()})
    with pytest.raises(ETLCompilationError, match="staging.new"):
        ETLCompiler(catalog).compile(MissingSourcePipeline)


def test_catalog_non_overwrite_target_missing_raises() -> None:
    """Non-OVERWRITE target that doesn't exist raises ETLCompilationError."""

    class NonOverwriteStep(ETLStep[RunParams]):
        target = IntoTable("staging.missing").replace()

        def execute(self, params: RunParams) -> Any:
            return None

    class P(ETLPipeline[RunParams]):
        class _Proc(ETLProcess[RunParams]):
            steps = [NonOverwriteStep]

        processes = [_Proc]

    catalog = StubCatalog(tables={})
    with pytest.raises(ETLCompilationError, match="staging.missing"):
        ETLCompiler(catalog).compile(P)


def test_catalog_parallel_steps_share_pre_group_will_create() -> None:
    """Steps in a parallel group do NOT see each other's OVERWRITE creates."""

    class ParallelCreate(ETLStep[RunParams]):
        target = IntoTable("staging.parallel_new").replace(schema=SchemaMode.OVERWRITE)

        def execute(self, params: RunParams) -> Any:
            return None

    class ParallelConsume(ETLStep[RunParams]):
        sources = Sources(data=FromTable("staging.parallel_new"))
        target = IntoTable("mart.parallel_out").replace()

        def execute(self, params: RunParams, *, data: Any) -> Any:
            return data

    class PipelineParallelCreateConsume(ETLPipeline[RunParams]):
        class _Proc(ETLProcess[RunParams]):
            steps = [[ParallelCreate, ParallelConsume]]

        processes = [_Proc]

    catalog = StubCatalog(tables={"mart.parallel_out": ()})
    with pytest.raises(ETLCompilationError, match="staging.parallel_new"):
        ETLCompiler(catalog).compile(PipelineParallelCreateConsume)


def test_catalog_sequential_step_sees_prior_parallel_creates() -> None:
    """A sequential step after a parallel group sees all creates from that group."""

    class ParallelNewA(ETLStep[RunParams]):
        target = IntoTable("staging.pa").replace(schema=SchemaMode.OVERWRITE)

        def execute(self, params: RunParams) -> Any:
            return None

    class ParallelNewB(ETLStep[RunParams]):
        target = IntoTable("staging.pb").replace(schema=SchemaMode.OVERWRITE)

        def execute(self, params: RunParams) -> Any:
            return None

    class ConsumeAB(ETLStep[RunParams]):
        sources = Sources(a=FromTable("staging.pa"), b=FromTable("staging.pb"))
        target = IntoTable("mart.agg").replace()

        def execute(self, params: RunParams, *, a: Any, b: Any) -> Any:
            return a

    class Pipeline(ETLPipeline[RunParams]):
        class _Proc(ETLProcess[RunParams]):
            steps = [[ParallelNewA, ParallelNewB], ConsumeAB]

        processes = [_Proc]

    catalog = StubCatalog(tables={"mart.agg": ()})
    ETLCompiler(catalog).compile(Pipeline)
