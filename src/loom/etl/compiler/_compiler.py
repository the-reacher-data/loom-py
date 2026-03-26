"""ETL compiler — static validation and plan construction.

The compiler inspects ETL class declarations once at startup and produces
immutable :class:`~loom.etl.compiler._plan.PipelinePlan` /
:class:`~loom.etl.compiler._plan.ProcessPlan` /
:class:`~loom.etl.compiler._plan.StepPlan` objects.  No reflection occurs
after compilation.

Backend auto-detection
----------------------
The compiler reads the return-type annotation of ``execute()`` and maps
it to :class:`~loom.etl.compiler._plan.Backend`:

* ``polars.DataFrame``              → ``Backend.POLARS``
* ``pyspark.sql.DataFrame``         → ``Backend.SPARK``
* anything else / unannotated       → ``Backend.UNKNOWN``

Static validation
-----------------
* ``ETLStep[ParamsT]`` generic present
* ``execute()`` first positional param is typed as ``ParamsT``
* every source alias has a matching ``*``-only DataFrame param in ``execute()``
* every ``*``-only param in ``execute()`` has a matching source alias
* ``target`` is declared
* source forms are not mixed (caught earlier in ``__init_subclass__``)
* ``ETLProcess.steps`` entries are valid step types
* ``ETLPipeline.processes`` entries are valid process types
"""

from __future__ import annotations

import logging
import typing
from typing import Any, cast

from loom.etl._io import TableDiscovery
from loom.etl._pipeline import ETLPipeline
from loom.etl._process import ETLProcess
from loom.etl._source import Sources, SourceSet
from loom.etl._step import ETLStep, _SourceForm
from loom.etl._target import IntoFile, IntoTable, IntoTemp
from loom.etl.compiler._catalog_validator import validate_plan_catalog
from loom.etl.compiler._errors import ETLCompilationError
from loom.etl.compiler._plan import (
    Backend,
    ParallelProcessGroup,
    ParallelStepGroup,
    PipelinePlan,
    PipelineProcessNode,
    ProcessPlan,
    ProcessStepNode,
    SourceBinding,
    StepPlan,
    TargetBinding,
)
from loom.etl.compiler._structural import validate_execute_signature, validate_params_compat
from loom.etl.compiler._temp_validator import validate_plan_temps
from loom.etl.compiler._upsert_validator import validate_upsert_spec

_log = logging.getLogger(__name__)


class ETLCompiler:
    """Compiles ETL declarations into immutable execution plans.

    Compilation is idempotent per class — results are cached.  Call
    :meth:`compile` with the top-level :class:`~loom.etl.ETLPipeline`
    subclass to obtain a :class:`~loom.etl.compiler._plan.PipelinePlan`.

    Args:
        catalog: Optional :class:`~loom.etl._io.TableDiscovery` instance.
                 When provided, all ``TABLE`` sources and ``IntoTable`` targets
                 are validated against the catalog at compile time.

    Example::

        plan = ETLCompiler().compile(DailyOrdersPipeline)
        plan = ETLCompiler(catalog=HiveCatalog()).compile(DailyOrdersPipeline)
    """

    def __init__(self, catalog: TableDiscovery | None = None) -> None:
        self._catalog = catalog
        self._step_cache: dict[type[Any], StepPlan] = {}
        self._process_cache: dict[type[Any], ProcessPlan] = {}

    def compile(self, pipeline_type: type[ETLPipeline[Any]]) -> PipelinePlan:
        """Compile a pipeline and return its immutable plan.

        Args:
            pipeline_type: Concrete :class:`~loom.etl.ETLPipeline` subclass.

        Returns:
            Fully validated :class:`~loom.etl.compiler._plan.PipelinePlan`.

        Raises:
            ETLCompilationError: If any structural constraint is violated.
        """
        params_type = _require_params_type(pipeline_type, "ETLPipeline")
        _log.debug(
            "compile pipeline=%s processes=%d", pipeline_type.__name__, len(pipeline_type.processes)
        )
        nodes = tuple(
            self._compile_pipeline_item(item, pipeline_type, params_type)
            for item in pipeline_type.processes
        )
        plan = PipelinePlan(
            pipeline_type=pipeline_type,
            params_type=params_type,
            nodes=nodes,
        )
        validate_plan_temps(plan)
        if self._catalog is not None:
            validate_plan_catalog(plan, self._catalog)
        return plan

    def compile_process(self, process_type: type[ETLProcess[Any]]) -> ProcessPlan:
        """Compile a single process.

        Args:
            process_type: Concrete :class:`~loom.etl.ETLProcess` subclass.

        Returns:
            Validated :class:`~loom.etl.compiler._plan.ProcessPlan`.

        Raises:
            ETLCompilationError: If any structural constraint is violated.
        """
        plan = self._get_or_build_process(process_type)
        if self._catalog is not None:
            from loom.etl.compiler._catalog_validator import _validate_process

            _validate_process(plan, self._catalog, set())
        return plan

    def compile_step(self, step_type: type[ETLStep[Any]]) -> StepPlan:
        """Compile a single step.

        Args:
            step_type: Concrete :class:`~loom.etl.ETLStep` subclass.

        Returns:
            Validated :class:`~loom.etl.compiler._plan.StepPlan`.

        Raises:
            ETLCompilationError: If any structural constraint is violated.
        """
        plan = self._get_or_build_step(step_type)
        if self._catalog is not None:
            from loom.etl.compiler._catalog_validator import _validate_catalog_tables

            _validate_catalog_tables(
                step_type, plan.source_bindings, plan.target_binding, self._catalog, set()
            )
        return plan

    def _compile_pipeline_item(
        self,
        item: Any,
        pipeline_type: type[Any],
        pipeline_params_type: type[Any],
    ) -> PipelineProcessNode:
        if isinstance(item, list):
            return ParallelProcessGroup(
                plans=cast(
                    "tuple[ProcessPlan, ...]",
                    tuple(
                        self._compile_pipeline_item(sub, pipeline_type, pipeline_params_type)
                        for sub in item
                    ),
                )
            )
        if not (isinstance(item, type) and issubclass(item, ETLProcess)):
            raise ETLCompilationError.invalid_process_item(pipeline_type, item)
        plan = self._get_or_build_process(item)
        validate_params_compat(item, plan.params_type, pipeline_params_type)
        return plan

    def _build_process_plan(self, process_type: type[ETLProcess[Any]]) -> ProcessPlan:
        params_type = _require_params_type(process_type, "ETLProcess")
        nodes = tuple(
            self._compile_process_item(item, process_type, params_type)
            for item in process_type.steps
        )
        return ProcessPlan(
            process_type=process_type,
            params_type=params_type,
            nodes=nodes,
        )

    def _compile_process_item(
        self,
        item: Any,
        process_type: type[Any],
        process_params_type: type[Any],
    ) -> ProcessStepNode:
        if isinstance(item, list):
            return ParallelStepGroup(
                plans=cast(
                    "tuple[StepPlan, ...]",
                    tuple(
                        self._compile_process_item(sub, process_type, process_params_type)
                        for sub in item
                    ),
                )
            )
        if not (isinstance(item, type) and issubclass(item, ETLStep)):
            raise ETLCompilationError.invalid_step_item(process_type, item)
        plan = self._get_or_build_step(item)
        validate_params_compat(item, plan.params_type, process_params_type)
        return plan

    def _get_or_build_process(self, process_type: type[ETLProcess[Any]]) -> ProcessPlan:
        if process_type in self._process_cache:
            _log.debug("compile cache hit process=%s", process_type.__name__)
            return self._process_cache[process_type]
        plan = self._build_process_plan(process_type)
        self._process_cache[process_type] = plan
        return plan

    def _get_or_build_step(self, step_type: type[ETLStep[Any]]) -> StepPlan:
        if step_type in self._step_cache:
            _log.debug("compile cache hit step=%s", step_type.__name__)
            return self._step_cache[step_type]
        plan = self._build_step_plan(step_type)
        self._step_cache[step_type] = plan
        return plan

    def _build_step_plan(self, step_type: type[ETLStep[Any]]) -> StepPlan:
        params_type = _require_params_type(step_type, "ETLStep")
        source_bindings = self._resolve_source_bindings(step_type)
        target_binding = self._resolve_target_binding(step_type)
        backend = _detect_backend(step_type)
        _log.debug(
            "compile step=%s backend=%s sources=%d",
            step_type.__name__,
            backend,
            len(source_bindings),
        )
        validate_execute_signature(step_type, params_type, source_bindings)
        return StepPlan(
            step_type=step_type,
            params_type=params_type,
            source_bindings=source_bindings,
            target_binding=target_binding,
            backend=backend,
        )

    def _resolve_source_bindings(self, step_type: type[ETLStep[Any]]) -> tuple[SourceBinding, ...]:
        match step_type._source_form:
            case _SourceForm.NONE:
                return ()
            case _SourceForm.INLINE:
                return tuple(
                    SourceBinding(alias=alias, spec=src._to_spec(alias))
                    for alias, src in step_type._inline_sources.items()
                )
            case _SourceForm.GROUPED:
                return _bindings_from_grouped(step_type)

    def _resolve_target_binding(self, step_type: type[ETLStep[Any]]) -> TargetBinding:
        target = step_type.target
        if target is None:
            raise ETLCompilationError.missing_target(step_type)
        if not isinstance(target, (IntoTable, IntoFile, IntoTemp)):
            raise ETLCompilationError.invalid_target_type(step_type)
        spec = target._to_spec()
        validate_upsert_spec(step_type, spec)
        return TargetBinding(spec=spec)


def _bindings_from_grouped(step_type: type[ETLStep[Any]]) -> tuple[SourceBinding, ...]:
    """Extract source bindings from a GROUPED-form step (Sources or SourceSet)."""
    match step_type.sources:
        case Sources() | SourceSet() as grouped:
            return tuple(SourceBinding(alias=spec.alias, spec=spec) for spec in grouped._to_specs())
        case _:
            raise ETLCompilationError.invalid_sources_type(step_type)


def _require_params_type(cls: type[Any], kind: str) -> type[Any]:
    pt = getattr(cls, "_params_type", None)
    if pt is None:
        raise ETLCompilationError.missing_generic_param(cls, kind)
    return cast(type[Any], pt)


def _detect_backend(step_type: type[ETLStep[Any]]) -> Backend:
    try:
        hints = typing.get_type_hints(step_type.execute)
    except Exception:
        return Backend.UNKNOWN

    return_type = hints.get("return")
    if return_type is None:
        return Backend.UNKNOWN

    qualname = (
        f"{getattr(return_type, '__module__', '')}.{getattr(return_type, '__qualname__', '')}"
    ).lower()
    if "polars" in qualname and ("lazyframe" in qualname or "dataframe" in qualname):
        return Backend.POLARS
    if "pyspark" in qualname and "dataframe" in qualname:
        return Backend.SPARK
    return Backend.UNKNOWN
