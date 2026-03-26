"""Compile-time FromTemp forward-reference validation."""

from __future__ import annotations

from loom.etl._source import SourceKind
from loom.etl.compiler._errors import ETLCompilationError
from loom.etl.compiler._plan import (
    ParallelProcessGroup,
    ParallelStepGroup,
    PipelinePlan,
    PipelineProcessNode,
    ProcessPlan,
    ProcessStepNode,
    StepPlan,
    TargetBinding,
)


def validate_plan_temps(plan: PipelinePlan) -> None:
    """Walk the plan validating that every FromTemp has a prior IntoTemp.

    Mirrors the will_create logic used for catalog validation: parallel
    groups share the pre-group snapshot and merge their produces after.

    Args:
        plan: Fully compiled pipeline plan.

    Raises:
        ETLCompilationError: When a FromTemp references a name not yet produced.
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
        _validate_process_node_temps(node, will_temp)


def _validate_process_node_temps(
    node: ProcessStepNode,
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
            temp_name = spec.temp_name or ""
            raise ETLCompilationError.temp_not_produced(step.step_type, binding.alias, temp_name)


def _register_temp_target(target_binding: TargetBinding, will_temp: set[str]) -> None:
    spec = target_binding.spec
    if spec.temp_name is not None:
        will_temp.add(spec.temp_name)
