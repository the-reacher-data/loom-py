"""Immutable ETL compiled plan types.

All plan objects are frozen dataclasses — zero mutation after compilation,
zero reflection at execution time.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

from loom.etl.io._source import SourceSpec
from loom.etl.io.target import TargetSpec


@dataclass(frozen=True)
class SourceBinding:
    """Compiled binding between a source alias and its normalized spec.

    Args:
        alias:  Name matching the ``execute()`` keyword parameter.
        spec:   Normalized source specification.
    """

    alias: str
    spec: SourceSpec


@dataclass(frozen=True)
class TargetBinding:
    """Compiled binding for the step target.

    Args:
        spec: Normalized target specification.
    """

    spec: TargetSpec


@dataclass(frozen=True)
class StepPlan:
    """Compiled plan for a single :class:`~loom.etl.ETLStep`.

    Args:
        step_type:       The concrete ``ETLStep`` subclass.
        params_type:     The ``ParamsT`` generic argument.
        source_bindings: Ordered source alias → spec bindings.
        target_binding:  Compiled target.
    """

    step_type: type[Any]
    params_type: type[Any]
    source_bindings: tuple[SourceBinding, ...]
    target_binding: TargetBinding


@dataclass(frozen=True)
class ParallelStepGroup:
    """A group of :class:`StepPlan` items that execute concurrently.

    Args:
        plans: Step plans to run in parallel.
    """

    plans: tuple[StepPlan, ...]


# A process node is either one step or a parallel group of steps.
ProcessStepNode = StepPlan | ParallelStepGroup


@dataclass(frozen=True)
class ProcessPlan:
    """Compiled plan for an :class:`~loom.etl.ETLProcess`.

    Args:
        process_type: The concrete ``ETLProcess`` subclass.
        params_type:  The ``ParamsT`` generic argument.
        nodes:        Ordered sequence of step nodes (sequential or parallel groups).
    """

    process_type: type[Any]
    params_type: type[Any]
    nodes: tuple[ProcessStepNode, ...]


@dataclass(frozen=True)
class ParallelProcessGroup:
    """A group of :class:`ProcessPlan` items that execute concurrently.

    Args:
        plans: Process plans to run in parallel.
    """

    plans: tuple[ProcessPlan, ...]


# A pipeline node is either one process or a parallel group of processes.
PipelineProcessNode = ProcessPlan | ParallelProcessGroup


@dataclass(frozen=True)
class PipelinePlan:
    """Compiled plan for an :class:`~loom.etl.ETLPipeline`.

    Args:
        pipeline_type: The concrete ``ETLPipeline`` subclass.
        params_type:   The ``ParamsT`` generic argument.
        nodes:         Ordered sequence of process nodes.
    """

    pipeline_type: type[Any]
    params_type: type[Any]
    nodes: tuple[PipelineProcessNode, ...]


def iter_processes(plan: PipelinePlan) -> Iterator[ProcessPlan]:
    """Yield every concrete process plan in pipeline execution order.

    Flattens top-level :class:`ParallelProcessGroup` nodes while preserving
    the declared order within each group.
    """
    for node in plan.nodes:
        match node:
            case ProcessPlan():
                yield node
            case ParallelProcessGroup(plans=plans):
                yield from plans


def iter_steps_in_process(plan: ProcessPlan) -> Iterator[StepPlan]:
    """Yield every concrete step plan in process execution order.

    Flattens :class:`ParallelStepGroup` nodes while preserving the declared
    order within each group.
    """
    for node in plan.nodes:
        match node:
            case StepPlan():
                yield node
            case ParallelStepGroup(plans=plans):
                yield from plans


def iter_all_steps(plan: PipelinePlan) -> Iterator[StepPlan]:
    """Yield every concrete step plan in a pipeline."""
    for proc in iter_processes(plan):
        yield from iter_steps_in_process(proc)


def _map_pipeline_nodes(
    nodes: tuple[PipelineProcessNode, ...],
    fn: Callable[[ProcessPlan], ProcessPlan | None],
) -> tuple[PipelineProcessNode, ...]:
    """Map/Filter process nodes while preserving parallel-group shape.

    ``fn`` receives concrete :class:`ProcessPlan` items. Returning ``None``
    drops the item. Parallel groups with a single kept process are collapsed
    into a plain :class:`ProcessPlan`.
    """
    result: list[PipelineProcessNode] = []
    for node in nodes:
        match node:
            case ProcessPlan():
                if mapped := fn(node):
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
    """Map/Filter step nodes while preserving parallel-group shape.

    ``fn`` receives concrete :class:`StepPlan` items. Returning ``None``
    drops the item. Parallel groups with a single kept step are collapsed
    into a plain :class:`StepPlan`.
    """
    result: list[ProcessStepNode] = []
    for node in nodes:
        match node:
            case StepPlan():
                if mapped := fn(node):
                    result.append(mapped)
            case ParallelStepGroup(plans=plans):
                kept = tuple(step for step in (fn(step) for step in plans) if step is not None)
                if len(kept) == 1:
                    result.append(kept[0])
                elif kept:
                    result.append(ParallelStepGroup(plans=kept))
    return tuple(result)


def visit_pipeline_nodes(
    nodes: tuple[PipelineProcessNode, ...],
    on_process: Callable[[ProcessPlan], None],
    *,
    on_parallel_group: Callable[[tuple[ProcessPlan, ...]], None] | None = None,
) -> None:
    """Visit pipeline nodes with optional custom handling for parallel groups.

    Args:
        nodes: Pipeline-level nodes to visit.
        on_process: Callback for each concrete :class:`ProcessPlan`.
        on_parallel_group: Optional callback for each
            :class:`ParallelProcessGroup`. When omitted, groups are flattened
            and ``on_process`` is called for every process in the group.
    """
    for node in nodes:
        match node:
            case ProcessPlan():
                on_process(node)
            case ParallelProcessGroup(plans=plans):
                if on_parallel_group is None:
                    for proc in plans:
                        on_process(proc)
                else:
                    on_parallel_group(plans)


def visit_process_nodes(
    nodes: tuple[ProcessStepNode, ...],
    on_step: Callable[[StepPlan], None],
    *,
    on_parallel_group: Callable[[tuple[StepPlan, ...]], None] | None = None,
) -> None:
    """Visit process nodes with optional custom handling for parallel groups.

    Args:
        nodes: Process-level nodes to visit.
        on_step: Callback for each concrete :class:`StepPlan`.
        on_parallel_group: Optional callback for each
            :class:`ParallelStepGroup`. When omitted, groups are flattened and
            ``on_step`` is called for every step in the group.
    """
    for node in nodes:
        match node:
            case StepPlan():
                on_step(node)
            case ParallelStepGroup(plans=plans):
                if on_parallel_group is None:
                    for step in plans:
                        on_step(step)
                else:
                    on_parallel_group(plans)
