"""Compile-time catalog table existence validation."""

from __future__ import annotations

from typing import Any

from loom.etl._io import TableDiscovery
from loom.etl._source import SourceKind
from loom.etl._target import SchemaMode
from loom.etl.compiler._errors import ETLCompilationError
from loom.etl.compiler._plan import (
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


def _validate_catalog_tables(
    step_type: type[Any],
    source_bindings: tuple[SourceBinding, ...],
    target_binding: TargetBinding,
    catalog: TableDiscovery,
    will_create: set[str],
) -> None:
    for binding in source_bindings:
        spec = binding.spec
        if (
            spec.kind is SourceKind.TABLE
            and spec.table_ref is not None
            and spec.table_ref.ref not in will_create
            and not catalog.exists(spec.table_ref)
        ):
            raise ETLCompilationError.unknown_source_table(
                step_type, binding.alias, spec.table_ref.ref
            )

    target_spec = target_binding.spec
    if (
        target_spec.table_ref is not None
        and target_spec.schema_mode is not SchemaMode.OVERWRITE
        and target_spec.table_ref.ref not in will_create
        and not catalog.exists(target_spec.table_ref)
    ):
        raise ETLCompilationError.unknown_target_table(step_type, target_spec.table_ref.ref)
