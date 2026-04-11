"""Plan filtering helpers used by ETLRunner.run(include=...)."""

from __future__ import annotations

from collections.abc import Callable

from loom.etl.compiler._plan import (
    ParallelProcessGroup,
    ParallelStepGroup,
    PipelinePlan,
    PipelineProcessNode,
    ProcessPlan,
    ProcessStepNode,
    StepPlan,
    iter_all_steps,
    iter_processes,
)
from loom.etl.runner.errors import InvalidStageError


def _filter_plan(plan: PipelinePlan, include: frozenset[str]) -> PipelinePlan:
    """Return a new :class:`PipelinePlan` keeping only nodes matched by *include*.

    Args:
        plan: Fully compiled pipeline plan.
        include: Set of step or process class names to keep.

    Returns:
        Filtered :class:`PipelinePlan`.

    Raises:
        InvalidStageError: When any name in *include* does not exist in the plan.
    """
    unknown = include - _collect_all_names(plan)
    if unknown:
        raise InvalidStageError(frozenset(unknown))
    nodes = _map_pipeline_nodes(plan.nodes, lambda proc: _filter_process(proc, include))
    return PipelinePlan(
        pipeline_type=plan.pipeline_type,
        params_type=plan.params_type,
        nodes=nodes,
    )


def _filter_process(plan: ProcessPlan, include: frozenset[str]) -> ProcessPlan | None:
    if plan.process_type.__name__ in include:
        return plan
    nodes = _map_process_nodes(
        plan.nodes,
        lambda step: step if step.step_type.__name__ in include else None,
    )
    if not nodes:
        return None
    return ProcessPlan(
        process_type=plan.process_type,
        params_type=plan.params_type,
        nodes=nodes,
    )


def _collect_all_names(plan: PipelinePlan) -> frozenset[str]:
    """Collect every step and process class name reachable from *plan*."""
    names = {proc.process_type.__name__ for proc in iter_processes(plan)}
    names.update(step.step_type.__name__ for step in iter_all_steps(plan))
    return frozenset(names)


def _map_pipeline_nodes(
    nodes: tuple[PipelineProcessNode, ...],
    fn: Callable[[ProcessPlan], ProcessPlan | None],
) -> tuple[PipelineProcessNode, ...]:
    """Map/filter process nodes while preserving parallel-group shape."""
    result: list[PipelineProcessNode] = []
    for node in nodes:
        match node:
            case ProcessPlan():
                mapped = fn(node)
                if mapped is not None:
                    result.append(mapped)
            case ParallelProcessGroup(plans=plans):
                kept = tuple(proc for proc in (fn(proc) for proc in plans) if proc is not None)
                if len(kept) == 1:
                    result.append(kept[0])
                elif kept:
                    result.append(ParallelProcessGroup(plans=kept))
    return tuple(result)


def _map_process_nodes(
    nodes: tuple[ProcessStepNode, ...],
    fn: Callable[[StepPlan], StepPlan | None],
) -> tuple[ProcessStepNode, ...]:
    """Map/filter step nodes while preserving parallel-group shape."""
    result: list[ProcessStepNode] = []
    for node in nodes:
        match node:
            case StepPlan():
                mapped = fn(node)
                if mapped is not None:
                    result.append(mapped)
            case ParallelStepGroup(plans=plans):
                kept = tuple(step for step in (fn(step) for step in plans) if step is not None)
                if len(kept) == 1:
                    result.append(kept[0])
                elif kept:
                    result.append(ParallelStepGroup(plans=kept))
    return tuple(result)
