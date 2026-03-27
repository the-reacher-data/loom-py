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
from loom.etl.io._target import SchemaMode
from loom.etl.testing import StubCatalog


class RunParams(ETLParams):  # type: ignore[misc]
    run_date: date
    countries: tuple[str, ...]


class ExtractStep(ETLStep[RunParams]):
    sources = Sources(orders=FromTable("raw.orders"))
    target = IntoTable("staging.orders").replace()

    def execute(self, params: RunParams, *, orders: Any) -> Any:  # type: ignore[override]
        return orders


class EnrichAStep(ETLStep[RunParams]):
    sources = Sources(orders=FromTable("staging.orders"))
    target = IntoTable("staging.enriched_a").replace()

    def execute(self, params: RunParams, *, orders: Any) -> Any:  # type: ignore[override]
        return orders


class EnrichBStep(ETLStep[RunParams]):
    sources = Sources(orders=FromTable("staging.orders"))
    target = IntoTable("staging.enriched_b").replace()

    def execute(self, params: RunParams, *, orders: Any) -> Any:  # type: ignore[override]
        return orders


class AggStep(ETLStep[RunParams]):
    sources = Sources(
        enriched_a=FromTable("staging.enriched_a"),
        enriched_b=FromTable("staging.enriched_b"),
    )
    target = IntoTable("mart.summary").replace()

    def execute(self, params: RunParams, *, enriched_a: Any, enriched_b: Any) -> Any:  # type: ignore[override]
        return enriched_a


class NoSourceStep(ETLStep[RunParams]):
    target = IntoTable("staging.calendar").replace()

    def execute(self, params: RunParams) -> Any:  # type: ignore[override]
        return None


class UntypedStep(ETLStep[RunParams]):
    sources = Sources(orders=FromTable("raw.x"))
    target = IntoTable("staging.x").replace()

    def execute(self, params, *, orders):  # type: ignore[override,no-untyped-def]
        return orders


class NoTarget(ETLStep[RunParams]):
    sources = Sources(orders=FromTable("raw.orders"))

    def execute(self, params: RunParams, *, orders: Any) -> Any:  # type: ignore[override]
        return orders


class MissingFrame(ETLStep[RunParams]):
    sources = Sources(
        orders=FromTable("raw.orders"),
        customers=FromTable("raw.customers"),
    )
    target = IntoTable("staging.x").replace()

    def execute(self, params: RunParams, *, orders: Any) -> Any:  # type: ignore[override]
        return orders


class ExtraFrame(ETLStep[RunParams]):
    sources = Sources(orders=FromTable("raw.orders"))
    target = IntoTable("staging.x").replace()

    def execute(self, params: RunParams, *, orders: Any, ghost: Any) -> Any:  # type: ignore[override]
        return orders


class NoParams(ETLStep[RunParams]):
    target = IntoTable("staging.x").replace()

    def execute(self) -> Any:  # type: ignore[override]
        return None


class BareStep(ETLStep):  # type: ignore[type-arg]
    target = IntoTable("staging.x").replace()

    def execute(self, params: Any) -> Any:  # type: ignore[override]
        return None


class _UnresolvableReturnStep(ETLStep[RunParams]):
    """Step whose execute return annotation references an undefined name.

    With ``from __future__ import annotations``, annotations are stored as
    strings and evaluated lazily by ``get_type_hints``.  This step intentionally
    uses an undefined name so that ``get_type_hints`` raises ``NameError``,
    exercising the ``_detect_backend`` error-handling path.
    """

    target = IntoTable("staging.unresolvable").replace()

    def execute(self, params: RunParams) -> _UndefinedType:  # type: ignore[name-defined]  # noqa: F821
        return None  # type: ignore[return-value]


class StagingProcess(ETLProcess[RunParams]):
    steps = [ExtractStep, [EnrichAStep, EnrichBStep], AggStep]


class BadProcess(ETLProcess[RunParams]):
    steps = ["not_a_step"]


class DailyPipeline(ETLPipeline[RunParams]):
    processes = [StagingProcess]


class ParallelPipeline(ETLPipeline[RunParams]):
    processes = [[StagingProcess, StagingProcess]]


class BadPipeline(ETLPipeline[RunParams]):
    processes = ["not_a_process"]


class NarrowParams(ETLParams):  # type: ignore[misc]
    run_date: date


class NarrowStep(ETLStep[NarrowParams]):
    target = IntoTable("staging.narrow").replace()

    def execute(self, params: NarrowParams) -> Any:  # type: ignore[override]
        return None


class NarrowProcess(ETLProcess[NarrowParams]):
    steps = [NarrowStep]


class WideProcess(ETLProcess[RunParams]):
    steps = [NarrowStep]


class WidePipeline(ETLPipeline[RunParams]):
    processes = [NarrowProcess]


class UnrelatedParams(ETLParams):  # type: ignore[misc]
    region: str


class UnrelatedStep(ETLStep[UnrelatedParams]):
    target = IntoTable("staging.unrelated").replace()

    def execute(self, params: UnrelatedParams) -> Any:  # type: ignore[override]
        return None


class UnrelatedProcess(ETLProcess[UnrelatedParams]):
    steps = [UnrelatedStep]


class MixedProcess(ETLProcess[RunParams]):
    steps = [UnrelatedStep]


class MixedPipeline(ETLPipeline[RunParams]):
    processes = [UnrelatedProcess]


class ParallelMixedProcess(ETLProcess[RunParams]):
    steps = [[NarrowStep, UnrelatedStep]]


class CreateStep(ETLStep[RunParams]):
    target = IntoTable("staging.new").replace(schema=SchemaMode.OVERWRITE)

    def execute(self, params: RunParams) -> Any:  # type: ignore[override]
        return None


class ConsumeStep(ETLStep[RunParams]):
    sources = Sources(data=FromTable("staging.new"))
    target = IntoTable("mart.out").replace()

    def execute(self, params: RunParams, *, data: Any) -> Any:  # type: ignore[override]
        return data


class CreateThenConsumePipeline(ETLPipeline[RunParams]):
    class _Proc(ETLProcess[RunParams]):
        steps = [CreateStep, ConsumeStep]

    processes = [_Proc]


class MissingSourcePipeline(ETLPipeline[RunParams]):
    class _Proc(ETLProcess[RunParams]):
        steps = [ConsumeStep]

    processes = [_Proc]


class NonOverwriteStep(ETLStep[RunParams]):
    target = IntoTable("staging.missing").replace()

    def execute(self, params: RunParams) -> Any:  # type: ignore[override]
        return None


class NonOverwritePipeline(ETLPipeline[RunParams]):
    class _Proc(ETLProcess[RunParams]):
        steps = [NonOverwriteStep]

    processes = [_Proc]


class ParallelCreate(ETLStep[RunParams]):
    target = IntoTable("staging.parallel_new").replace(schema=SchemaMode.OVERWRITE)

    def execute(self, params: RunParams) -> Any:  # type: ignore[override]
        return None


class ParallelConsume(ETLStep[RunParams]):
    sources = Sources(data=FromTable("staging.parallel_new"))
    target = IntoTable("mart.parallel_out").replace()

    def execute(self, params: RunParams, *, data: Any) -> Any:  # type: ignore[override]
        return data


class PipelineParallelCreateConsume(ETLPipeline[RunParams]):
    class _Proc(ETLProcess[RunParams]):
        steps = [[ParallelCreate, ParallelConsume]]

    processes = [_Proc]


class ParallelNewA(ETLStep[RunParams]):
    target = IntoTable("staging.pa").replace(schema=SchemaMode.OVERWRITE)

    def execute(self, params: RunParams) -> Any:  # type: ignore[override]
        return None


class ParallelNewB(ETLStep[RunParams]):
    target = IntoTable("staging.pb").replace(schema=SchemaMode.OVERWRITE)

    def execute(self, params: RunParams) -> Any:  # type: ignore[override]
        return None


class ConsumeAB(ETLStep[RunParams]):
    sources = Sources(a=FromTable("staging.pa"), b=FromTable("staging.pb"))
    target = IntoTable("mart.agg").replace()

    def execute(self, params: RunParams, *, a: Any, b: Any) -> Any:  # type: ignore[override]
        return a


class PipelineAfterParallelCreate(ETLPipeline[RunParams]):
    class _Proc(ETLProcess[RunParams]):
        steps = [[ParallelNewA, ParallelNewB], ConsumeAB]

    processes = [_Proc]


class TestCompileStep:
    @pytest.mark.parametrize(
        "step_type,expected_aliases,target_ref,expected_backend",
        [
            (ExtractStep, {"orders"}, "staging.orders", Backend.UNKNOWN),
            (AggStep, {"enriched_a", "enriched_b"}, "mart.summary", Backend.UNKNOWN),
            (NoSourceStep, set(), "staging.calendar", Backend.UNKNOWN),
            (UntypedStep, {"orders"}, "staging.x", Backend.UNKNOWN),
        ],
    )
    def test_compile_step_shape(
        self,
        step_type: type[ETLStep[RunParams]],
        expected_aliases: set[str],
        target_ref: str,
        expected_backend: Backend,
    ) -> None:
        plan = ETLCompiler().compile_step(step_type)
        assert plan.step_type is step_type
        assert plan.params_type is RunParams
        assert {binding.alias for binding in plan.source_bindings} == expected_aliases
        assert plan.target_binding.spec.table_ref is not None
        assert plan.target_binding.spec.table_ref.ref == target_ref
        assert plan.backend is expected_backend

    def test_compile_step_is_cached(self) -> None:
        compiler = ETLCompiler()
        first = compiler.compile_step(ExtractStep)
        second = compiler.compile_step(ExtractStep)
        assert first is second

    @pytest.mark.parametrize(
        "step_type,error",
        [
            (NoTarget, "target.*required"),
            (MissingFrame, "customers"),
            (ExtraFrame, "ghost"),
            (NoParams, "params"),
            (BareStep, "missing generic parameter"),
        ],
    )
    def test_compile_step_errors(
        self,
        step_type: type[ETLStep[Any]],
        error: str,
    ) -> None:
        with pytest.raises(ETLCompilationError, match=error):
            ETLCompiler().compile_step(step_type)


class TestCompileProcessAndPipeline:
    def test_compile_process_parallel_node(self) -> None:
        plan = ETLCompiler().compile_process(StagingProcess)
        assert plan.process_type is StagingProcess
        assert len(plan.nodes) == 3
        assert isinstance(plan.nodes[1], ParallelStepGroup)
        assert len(plan.nodes[1].plans) == 2

    def test_compile_process_invalid_item_raises(self) -> None:
        with pytest.raises(ETLCompilationError, match="ETLStep subclass"):
            ETLCompiler().compile_process(BadProcess)

    @pytest.mark.parametrize(
        "pipeline_type,is_parallel",
        [
            (DailyPipeline, False),
            (ParallelPipeline, True),
        ],
    )
    def test_compile_pipeline_shapes(
        self,
        pipeline_type: type[ETLPipeline[RunParams]],
        is_parallel: bool,
    ) -> None:
        plan = ETLCompiler().compile(pipeline_type)
        assert plan.pipeline_type is pipeline_type
        assert plan.params_type is RunParams
        assert len(plan.nodes) == 1
        assert isinstance(plan.nodes[0], ParallelProcessGroup) is is_parallel

    def test_compile_pipeline_invalid_process_raises(self) -> None:
        with pytest.raises(ETLCompilationError, match="ETLProcess subclass"):
            ETLCompiler().compile(BadPipeline)


class TestParamsCompatibility:
    @pytest.mark.parametrize(
        "process_type",
        [StagingProcess, WideProcess],
    )
    def test_process_params_compatible(self, process_type: type[ETLProcess[RunParams]]) -> None:
        assert ETLCompiler().compile_process(process_type) is not None

    def test_pipeline_params_compatible(self) -> None:
        assert ETLCompiler().compile(WidePipeline) is not None

    @pytest.mark.parametrize(
        "process_type,error",
        [
            (MixedProcess, "region"),
            (MixedProcess, "UnrelatedStep"),
            (ParallelMixedProcess, "region"),
        ],
    )
    def test_process_params_incompatible(
        self,
        process_type: type[ETLProcess[RunParams]],
        error: str,
    ) -> None:
        with pytest.raises(ETLCompilationError, match=error):
            ETLCompiler().compile_process(process_type)

    def test_pipeline_params_incompatible(self) -> None:
        with pytest.raises(ETLCompilationError, match="region"):
            ETLCompiler().compile(MixedPipeline)


class TestCatalogValidation:
    @pytest.mark.parametrize(
        "pipeline_type,catalog_tables,error",
        [
            (
                CreateThenConsumePipeline,
                {"raw.orders": (), "mart.out": ()},
                None,
            ),
            (
                CreateThenConsumePipeline,
                {"mart.out": ()},
                None,
            ),
            (
                MissingSourcePipeline,
                {"mart.out": ()},
                "staging.new",
            ),
            (
                NonOverwritePipeline,
                {},
                "staging.missing",
            ),
            (
                PipelineParallelCreateConsume,
                {"mart.parallel_out": ()},
                "staging.parallel_new",
            ),
            (
                PipelineAfterParallelCreate,
                {"mart.agg": ()},
                None,
            ),
        ],
    )
    def test_catalog_validation_scenarios(
        self,
        pipeline_type: type[ETLPipeline[RunParams]],
        catalog_tables: dict[str, tuple[str, ...]],
        error: str | None,
    ) -> None:
        compiler = ETLCompiler(StubCatalog(tables=catalog_tables))
        if error is None:
            compiler.compile(pipeline_type)
            return
        with pytest.raises(ETLCompilationError, match=error):
            compiler.compile(pipeline_type)


class TestDetectBackend:
    def test_unresolvable_annotation_yields_unknown_backend(self) -> None:
        plan = ETLCompiler().compile_step(_UnresolvableReturnStep)
        assert plan.backend is Backend.UNKNOWN
