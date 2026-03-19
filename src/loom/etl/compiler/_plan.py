"""Immutable ETL compiled plan types.

All plan objects are frozen dataclasses — zero mutation after compilation,
zero reflection at execution time.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from loom.etl._source import SourceSpec
from loom.etl._target import TargetSpec


class Backend(StrEnum):
    """Execution backend detected from the ``execute()`` return type."""

    POLARS = "polars"
    SPARK = "spark"
    UNKNOWN = "unknown"


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
        backend:         Auto-detected execution backend.
    """

    step_type: type[Any]
    params_type: type[Any]
    source_bindings: tuple[SourceBinding, ...]
    target_binding: TargetBinding
    backend: Backend


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
