"""Plan filtering helpers used by ETLRunner.run(include=...)."""

from __future__ import annotations

from loom.etl.compiler._plan import (
    ParallelProcessGroup,
    ParallelStepGroup,
    PipelinePlan,
    PipelineProcessNode,
    ProcessPlan,
    ProcessStepNode,
    StepPlan,
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
    nodes = _filter_pipeline_nodes(plan.nodes, include)
    return PipelinePlan(
        pipeline_type=plan.pipeline_type,
        params_type=plan.params_type,
        nodes=nodes,
    )


def _filter_pipeline_nodes(
    nodes: tuple[PipelineProcessNode, ...],
    include: frozenset[str],
) -> tuple[PipelineProcessNode, ...]:
    result: list[PipelineProcessNode] = []
    for node in nodes:
        match node:
            case ProcessPlan():
                filtered = _filter_process(node, include)
                if filtered is not None:
                    result.append(filtered)
            case ParallelProcessGroup(plans=plans):
                kept = tuple(
                    proc
                    for proc in (_filter_process(proc, include) for proc in plans)
                    if proc is not None
                )
                if len(kept) == 1:
                    result.append(kept[0])
                elif kept:
                    result.append(ParallelProcessGroup(plans=kept))
    return tuple(result)


def _filter_process(plan: ProcessPlan, include: frozenset[str]) -> ProcessPlan | None:
    if plan.process_type.__name__ in include:
        return plan
    nodes = _filter_process_nodes(plan.nodes, include)
    if not nodes:
        return None
    return ProcessPlan(
        process_type=plan.process_type,
        params_type=plan.params_type,
        nodes=nodes,
    )


def _filter_process_nodes(
    nodes: tuple[ProcessStepNode, ...],
    include: frozenset[str],
) -> tuple[ProcessStepNode, ...]:
    result: list[ProcessStepNode] = []
    for node in nodes:
        match node:
            case StepPlan() if node.step_type.__name__ in include:
                result.append(node)
            case ParallelStepGroup(plans=plans):
                kept = tuple(step for step in plans if step.step_type.__name__ in include)
                if len(kept) == 1:
                    result.append(kept[0])
                elif kept:
                    result.append(ParallelStepGroup(plans=kept))
    return tuple(result)


def _collect_all_names(plan: PipelinePlan) -> frozenset[str]:
    """Collect every step and process class name reachable from *plan*."""
    names: set[str] = set()
    for node in plan.nodes:
        match node:
            case ProcessPlan():
                _collect_process_names(node, names)
            case ParallelProcessGroup(plans=plans):
                for proc in plans:
                    _collect_process_names(proc, names)
    return frozenset(names)


def _collect_process_names(plan: ProcessPlan, names: set[str]) -> None:
    names.add(plan.process_type.__name__)
    for node in plan.nodes:
        match node:
            case StepPlan():
                names.add(node.step_type.__name__)
            case ParallelStepGroup(plans=plans):
                for step in plans:
                    names.add(step.step_type.__name__)
