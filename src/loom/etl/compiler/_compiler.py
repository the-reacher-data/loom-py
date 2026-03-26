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

Static validation (this sprint)
--------------------------------
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

import inspect
import logging
import typing
from typing import Any, cast

import msgspec

from loom.etl._io import TableDiscovery
from loom.etl._pipeline import ETLPipeline
from loom.etl._process import ETLProcess
from loom.etl._source import SourceKind, Sources, SourceSet
from loom.etl._step import ETLStep, _SourceForm
from loom.etl._target import IntoFile, IntoTable, IntoTemp, SchemaMode, TargetSpec, WriteMode
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

_log = logging.getLogger(__name__)

_POLARS_DF_QUALNAME = "polars.dataframe.frame.DataFrame"
_SPARK_DF_QUALNAME = "pyspark.sql.dataframe.DataFrame"


class ETLCompilationError(Exception):
    """Raised when an ETL class fails structural validation.

    Args:
        message: Human-readable description of the failure.
    """


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
        _validate_plan_temps(plan)
        if self._catalog is not None:
            _validate_plan_catalog(plan, self._catalog)
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
            raise ETLCompilationError(
                f"{pipeline_type.__qualname__}.processes: "
                f"expected ETLProcess subclass or list thereof, got {item!r}"
            )
        plan = self._get_or_build_process(item)
        _validate_params_compat(item, plan.params_type, pipeline_params_type)
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
            raise ETLCompilationError(
                f"{process_type.__qualname__}.steps: "
                f"expected ETLStep subclass or list thereof, got {item!r}"
            )
        plan = self._get_or_build_step(item)
        _validate_params_compat(item, plan.params_type, process_params_type)
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
        self._validate_execute_signature(step_type, params_type, source_bindings)
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
            raise ETLCompilationError(
                f"{step_type.__qualname__}: 'target' is required but not declared"
            )
        if not isinstance(target, (IntoTable, IntoFile, IntoTemp)):
            raise ETLCompilationError(
                f"{step_type.__qualname__}: 'target' must be "
                "IntoTable(...), IntoFile(...), or IntoTemp(...)"
            )
        spec = target._to_spec()
        _validate_upsert_spec(step_type, spec)
        return TargetBinding(spec=spec)

    def _validate_execute_signature(
        self,
        step_type: type[ETLStep[Any]],
        params_type: type[Any],
        source_bindings: tuple[SourceBinding, ...],
    ) -> None:
        sig = inspect.signature(step_type.execute)
        params = list(sig.parameters.values())

        _validate_params_arg(step_type, params, params_type)

        kw_only = _collect_kw_only_frames(params)
        source_aliases = {b.alias for b in source_bindings}

        _check_missing_frames(step_type, source_aliases, kw_only)
        _check_extra_frames(step_type, source_aliases, kw_only)


def _bindings_from_grouped(step_type: type[ETLStep[Any]]) -> tuple[SourceBinding, ...]:
    """Extract source bindings from a GROUPED-form step (Sources or SourceSet)."""
    match step_type.sources:
        case Sources() | SourceSet() as grouped:
            return tuple(SourceBinding(alias=spec.alias, spec=spec) for spec in grouped._to_specs())
        case _:
            raise ETLCompilationError(
                f"{step_type.__qualname__}: 'sources' must be Sources(...) or a SourceSet instance"
            )


def _require_params_type(cls: type[Any], kind: str) -> type[Any]:
    pt = getattr(cls, "_params_type", None)
    if pt is None:
        raise ETLCompilationError(
            f"{cls.__qualname__}: missing generic parameter — "
            f"use {kind}[YourParams] not bare {kind}"
        )
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
    # Accept both LazyFrame (preferred) and DataFrame for Polars.
    if "polars" in qualname and ("lazyframe" in qualname or "dataframe" in qualname):
        return Backend.POLARS
    if "pyspark" in qualname and "dataframe" in qualname:
        return Backend.SPARK
    return Backend.UNKNOWN


def _validate_params_arg(
    step_type: type[Any],
    params: list[inspect.Parameter],
    params_type: type[Any],
) -> None:
    positional_kinds = {
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.POSITIONAL_ONLY,
    }
    positional = [p for p in params if p.kind in positional_kinds and p.name != "self"]
    if not positional:
        raise ETLCompilationError(
            f"{step_type.__qualname__}.execute: first parameter must be "
            f"'params: {params_type.__name__}'"
        )
    first = positional[0]
    if first.name != "params":
        raise ETLCompilationError(
            f"{step_type.__qualname__}.execute: first parameter must be named 'params', "
            f"got '{first.name}'"
        )


def _collect_kw_only_frames(
    params: list[inspect.Parameter],
) -> dict[str, inspect.Parameter]:
    return {p.name: p for p in params if p.kind is inspect.Parameter.KEYWORD_ONLY}


def _check_missing_frames(
    step_type: type[Any],
    source_aliases: set[str],
    kw_only: dict[str, inspect.Parameter],
) -> None:
    missing = source_aliases - set(kw_only)
    if missing:
        raise ETLCompilationError(
            f"{step_type.__qualname__}.execute: source(s) {sorted(missing)} "
            f"declared in sources but missing as keyword-only parameter(s) after '*'"
        )


def _check_extra_frames(
    step_type: type[Any],
    source_aliases: set[str],
    kw_only: dict[str, inspect.Parameter],
) -> None:
    extra = set(kw_only) - source_aliases
    if extra:
        raise ETLCompilationError(
            f"{step_type.__qualname__}.execute: parameter(s) {sorted(extra)} "
            f"declared after '*' but not found in sources"
        )


def _validate_params_compat(
    component_type: type[Any],
    component_params: type[Any],
    context_params: type[Any],
) -> None:
    """Raise if *component_params* requires fields absent from *context_params*.

    Passes when:
    * Both types are the same class.
    * *context_params* is a subclass of *component_params* (structural superset).
    * Every field declared in *component_params* is also present in *context_params*
      (duck-type / structural compatibility).

    Args:
        component_type:   The step or process class being validated (for error messages).
        component_params: The params type declared on the step/process.
        context_params:   The params type declared on the enclosing process/pipeline.

    Raises:
        ETLCompilationError: When required fields are missing from *context_params*.
    """
    if component_params is context_params:
        return
    if issubclass(context_params, component_params):
        return
    component_fields = {f.name for f in msgspec.structs.fields(component_params)}
    context_fields = {f.name for f in msgspec.structs.fields(context_params)}
    missing = component_fields - context_fields
    if missing:
        raise ETLCompilationError(
            f"{component_type.__qualname__} requires params fields "
            f"{sorted(missing)} not present in {context_params.__name__}"
        )


def _validate_plan_catalog(plan: PipelinePlan, catalog: TableDiscovery) -> None:
    """Walk the plan in execution order, validating all table references.

    Tables created by an OVERWRITE step are recorded in *will_create* so
    subsequent steps can reference them as sources without a catalog hit.
    Parallel groups share the pre-group snapshot and merge their creates
    after the group completes.
    """
    will_create: set[str] = set()
    for node in plan.nodes:
        _validate_pipeline_node(node, catalog, will_create)


def _validate_pipeline_node(
    node: PipelineProcessNode,
    catalog: TableDiscovery,
    will_create: set[str],
) -> None:
    match node:
        case ProcessPlan():
            _validate_process(node, catalog, will_create)
        case ParallelProcessGroup(plans=plans):
            snapshot = frozenset(will_create)
            group_creates: set[str] = set()
            for proc in plans:
                proc_creates = set(snapshot)
                _validate_process(proc, catalog, proc_creates)
                group_creates |= proc_creates - snapshot
            will_create |= group_creates


def _validate_process(
    plan: ProcessPlan,
    catalog: TableDiscovery,
    will_create: set[str],
) -> None:
    for node in plan.nodes:
        _validate_process_node(node, catalog, will_create)


def _validate_process_node(
    node: ProcessStepNode,
    catalog: TableDiscovery,
    will_create: set[str],
) -> None:
    match node:
        case StepPlan():
            _validate_catalog_tables(
                node.step_type, node.source_bindings, node.target_binding, catalog, will_create
            )
            _register_overwrite_target(node.target_binding, will_create)
        case ParallelStepGroup(plans=plans):
            snapshot = frozenset(will_create)
            group_creates: set[str] = set()
            for step in plans:
                # Each parallel step only sees the pre-group snapshot
                _validate_catalog_tables(
                    step.step_type,
                    step.source_bindings,
                    step.target_binding,
                    catalog,
                    set(snapshot),
                )
                _register_overwrite_target(step.target_binding, group_creates)
            will_create |= group_creates


def _register_overwrite_target(target_binding: TargetBinding, will_create: set[str]) -> None:
    spec = target_binding.spec
    if spec.table_ref is not None and spec.schema_mode is SchemaMode.OVERWRITE:
        will_create.add(spec.table_ref.ref)


def _validate_upsert_spec(step_type: type[Any], spec: TargetSpec) -> None:
    """Validate UPSERT-specific constraints at compile time.

    Args:
        step_type: The step class being validated (for error messages).
        spec:      Compiled target spec.

    Raises:
        ETLCompilationError: When ``upsert_keys`` is empty, ``exclude`` and
                             ``include`` are both set, or ``exclude`` overlaps
                             with ``upsert_keys``.
    """
    if spec.mode is not WriteMode.UPSERT:
        return
    _check_upsert_keys_non_empty(step_type, spec)
    _check_upsert_exclude_include_exclusive(step_type, spec)
    _check_upsert_exclude_keys_disjoint(step_type, spec)


def _check_upsert_keys_non_empty(step_type: type[Any], spec: TargetSpec) -> None:
    if not spec.upsert_keys:
        raise ETLCompilationError(
            f"{step_type.__qualname__}: upsert() requires at least one key column. "
            'Pass keys=("col",) to identify rows uniquely.'
        )


def _check_upsert_exclude_include_exclusive(step_type: type[Any], spec: TargetSpec) -> None:
    if spec.upsert_exclude and spec.upsert_include:
        raise ETLCompilationError(
            f"{step_type.__qualname__}: upsert() exclude= and include= are mutually exclusive. "
            "Use one or the other, not both."
        )


def _check_upsert_exclude_keys_disjoint(step_type: type[Any], spec: TargetSpec) -> None:
    overlap = frozenset(spec.upsert_exclude) & frozenset(spec.upsert_keys)
    if overlap:
        raise ETLCompilationError(
            f"{step_type.__qualname__}: upsert() exclude={sorted(overlap)} overlaps with "
            "upsert keys — key columns are always excluded from UPDATE SET."
        )


def _validate_plan_temps(plan: PipelinePlan) -> None:
    """Walk the plan validating that every FromTemp has a prior IntoTemp.

    Mirrors the ``will_create`` logic used for catalog validation: parallel
    groups share the pre-group snapshot and merge their produces after.
    """
    will_temp: set[str] = set()
    for node in plan.nodes:
        _validate_pipeline_node_temps(node, will_temp)


def _validate_pipeline_node_temps(
    node: PipelineProcessNode,
    will_temp: set[str],
) -> None:
    match node:
        case ProcessPlan():
            _validate_process_temps(node, will_temp)
        case ParallelProcessGroup(plans=plans):
            snapshot = frozenset(will_temp)
            group_produces: set[str] = set()
            for proc in plans:
                proc_temps = set(snapshot)
                _validate_process_temps(proc, proc_temps)
                group_produces |= proc_temps - snapshot
            will_temp |= group_produces


def _validate_process_temps(plan: ProcessPlan, will_temp: set[str]) -> None:
    for node in plan.nodes:
        _validate_process_node_temps(node, plan.process_type, will_temp)


def _validate_process_node_temps(
    node: ProcessStepNode,
    process_type: type[Any],
    will_temp: set[str],
) -> None:
    match node:
        case StepPlan():
            _check_temp_sources(node, will_temp)
            _register_temp_target(node.target_binding, will_temp)
        case ParallelStepGroup(plans=plans):
            snapshot = frozenset(will_temp)
            group_produces: set[str] = set()
            for step in plans:
                _check_temp_sources(step, set(snapshot))
                _register_temp_target(step.target_binding, group_produces)
            will_temp |= group_produces


def _check_temp_sources(step: StepPlan, will_temp: set[str]) -> None:
    for binding in step.source_bindings:
        spec = binding.spec
        if spec.kind is SourceKind.TEMP and spec.temp_name not in will_temp:
            raise ETLCompilationError(
                f"{step.step_type.__qualname__}: source '{binding.alias}' "
                f"references FromTemp({spec.temp_name!r}) but no prior "
                f"IntoTemp({spec.temp_name!r}) was found in the pipeline before this step"
            )


def _register_temp_target(target_binding: TargetBinding, will_temp: set[str]) -> None:
    spec = target_binding.spec
    if spec.temp_name is not None:
        will_temp.add(spec.temp_name)


def _validate_catalog_tables(
    step_type: type[Any],
    source_bindings: tuple[SourceBinding, ...],
    target_binding: TargetBinding,
    catalog: TableDiscovery,
    will_create: set[str],
) -> None:
    """Validate TABLE sources and IntoTable targets against the catalog.

    Sources are allowed when the table exists in the catalog OR was declared
    as an OVERWRITE target by a preceding step (*will_create*).

    Targets with ``schema_mode=OVERWRITE`` are exempt from the existence
    check — the writer will create them on first write.  All other write
    modes require the target to already exist or appear in *will_create*.
    """
    for binding in source_bindings:
        spec = binding.spec
        if (
            spec.kind is SourceKind.TABLE
            and spec.table_ref is not None
            and spec.table_ref.ref not in will_create
            and not catalog.exists(spec.table_ref)
        ):
            raise ETLCompilationError(
                f"{step_type.__qualname__}: source '{binding.alias}' "
                f"references unknown table '{spec.table_ref.ref}'"
            )

    target_spec = target_binding.spec
    if (
        target_spec.table_ref is not None
        and target_spec.schema_mode is not SchemaMode.OVERWRITE
        and target_spec.table_ref.ref not in will_create
        and not catalog.exists(target_spec.table_ref)
    ):
        raise ETLCompilationError(
            f"{step_type.__qualname__}: target references unknown table "
            f"'{target_spec.table_ref.ref}'"
        )
