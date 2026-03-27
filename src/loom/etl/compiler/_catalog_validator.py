"""Compile-time catalog table existence validation."""

from __future__ import annotations

from loom.etl.compiler._errors import ETLCompilationError
from loom.etl.compiler._plan import (
    ParallelProcessGroup,
    ParallelStepGroup,
    PipelinePlan,
    PipelineProcessNode,
    ProcessPlan,
    ProcessStepNode,
    StepPlan,
)
from loom.etl.io._source import SourceKind
from loom.etl.io._target import SchemaMode
from loom.etl.storage._io import TableDiscovery


def validate_plan_catalog(plan: PipelinePlan, catalog: TableDiscovery) -> None:
    """Walk the plan validating all table references against the catalog.

    Tables created by an OVERWRITE step are recorded in will_create so
    subsequent steps can reference them without a catalog hit.
    Parallel groups share the pre-group snapshot and merge their creates after.

    Args:
        plan:    Fully compiled pipeline plan.
        catalog: Table discovery implementation.

    Raises:
        ETLCompilationError: When a source or target table is not found.
    """
    will_create: set[str] = set()
    for node in plan.nodes:
        _walk_pipeline_node(node, catalog, will_create)


def _walk_pipeline_node(
    node: PipelineProcessNode,
    catalog: TableDiscovery,
    will_create: set[str],
) -> None:
    match node:
        case ProcessPlan():
            _walk_process(node, catalog, will_create)
        case ParallelProcessGroup(plans=plans):
            snapshot = frozenset(will_create)
            group_creates: set[str] = set()
            for proc in plans:
                proc_creates = set(snapshot)
                _walk_process(proc, catalog, proc_creates)
                group_creates |= proc_creates - snapshot
            will_create |= group_creates


def _walk_process(
    plan: ProcessPlan,
    catalog: TableDiscovery,
    will_create: set[str],
) -> None:
    for node in plan.nodes:
        _walk_process_node(node, catalog, will_create)


def _walk_process_node(
    node: ProcessStepNode,
    catalog: TableDiscovery,
    will_create: set[str],
) -> None:
    match node:
        case StepPlan():
            _validate_step(node, catalog, will_create)
        case ParallelStepGroup(plans=plans):
            snapshot = frozenset(will_create)
            group_creates: set[str] = set()
            for step in plans:
                _validate_step(step, catalog, set(snapshot))
                _track_overwrite(step, group_creates)
            will_create |= group_creates


def _validate_step(step: StepPlan, catalog: TableDiscovery, will_create: set[str]) -> None:
    _check_sources(step, catalog, will_create)
    _check_target(step, catalog, will_create)
    _track_overwrite(step, will_create)


def _check_sources(step: StepPlan, catalog: TableDiscovery, will_create: set[str]) -> None:
    for binding in step.source_bindings:
        spec = binding.spec
        if (
            spec.kind is SourceKind.TABLE
            and spec.table_ref is not None
            and spec.table_ref.ref not in will_create
            and not catalog.exists(spec.table_ref)
        ):
            raise ETLCompilationError.unknown_source_table(
                step.step_type, binding.alias, spec.table_ref.ref
            )


def _check_target(step: StepPlan, catalog: TableDiscovery, will_create: set[str]) -> None:
    spec = step.target_binding.spec
    if (
        spec.table_ref is not None
        and spec.schema_mode is not SchemaMode.OVERWRITE
        and spec.table_ref.ref not in will_create
        and not catalog.exists(spec.table_ref)
    ):
        raise ETLCompilationError.unknown_target_table(step.step_type, spec.table_ref.ref)


def _track_overwrite(step: StepPlan, will_create: set[str]) -> None:
    spec = step.target_binding.spec
    if spec.table_ref is not None and spec.schema_mode is SchemaMode.OVERWRITE:
        will_create.add(spec.table_ref.ref)
