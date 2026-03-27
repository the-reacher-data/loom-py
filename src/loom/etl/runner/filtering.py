"""Plan filtering helpers used by ETLRunner.run(include=...)."""

from __future__ import annotations

from loom.etl.compiler._plan import (
    PipelinePlan,
    ProcessPlan,
    _map_pipeline_nodes,
    _map_process_nodes,
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
