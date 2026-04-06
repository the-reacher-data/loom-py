"""Compile-time catalog table existence validation."""

from __future__ import annotations

from enum import Enum
from typing import Protocol, TypeGuard, cast

from loom.etl.compiler._errors import ETLCompilationError
from loom.etl.compiler._plan import (
    PipelinePlan,
    ProcessPlan,
    StepPlan,
    visit_pipeline_nodes,
    visit_process_nodes,
)
from loom.etl.io.source._specs import TableSourceSpec
from loom.etl.schema._table import TableRef
from loom.etl.storage._io import TableDiscovery

_SCHEMA_MODE_OVERWRITE = "overwrite"


class _TableRefLike(Protocol):
    ref: str


class _TableTargetSpecLike(Protocol):
    table_ref: _TableRefLike
    schema_mode: object


def validate_step_catalog(plan: StepPlan, catalog: TableDiscovery) -> None:
    """Validate a single step's sources and target against the catalog.

    Args:
        plan:    Compiled step plan.
        catalog: Table discovery implementation.

    Raises:
        ETLCompilationError: When a source or target table is not found.
    """
    _validate_step(plan, catalog, set())


def validate_process_catalog(plan: ProcessPlan, catalog: TableDiscovery) -> None:
    """Validate all steps in a process against the catalog.

    Args:
        plan:    Compiled process plan.
        catalog: Table discovery implementation.

    Raises:
        ETLCompilationError: When a source or target table is not found.
    """
    _walk_process(plan, catalog, set())


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
    visit_pipeline_nodes(
        plan.nodes,
        lambda proc: _walk_process(proc, catalog, will_create),
        on_parallel_group=lambda plans: _walk_parallel_process_group(plans, catalog, will_create),
    )


def _walk_process(
    plan: ProcessPlan,
    catalog: TableDiscovery,
    will_create: set[str],
) -> None:
    visit_process_nodes(
        plan.nodes,
        lambda step: _validate_step(step, catalog, will_create),
        on_parallel_group=lambda plans: _walk_parallel_step_group(plans, catalog, will_create),
    )


def _walk_parallel_process_group(
    plans: tuple[ProcessPlan, ...],
    catalog: TableDiscovery,
    will_create: set[str],
) -> None:
    # Each parallel branch starts from the same pre-group state.
    snapshot = frozenset(will_create)
    group_creates: set[str] = set()
    for proc in plans:
        proc_creates = set(snapshot)
        _walk_process(proc, catalog, proc_creates)
        # Merge only tables created within this branch.
        group_creates |= proc_creates - snapshot
    # Expose branch-created tables to subsequent sequential nodes.
    will_create |= group_creates


def _walk_parallel_step_group(
    plans: tuple[StepPlan, ...],
    catalog: TableDiscovery,
    will_create: set[str],
) -> None:
    # Validate each branch against the same incoming state.
    snapshot = frozenset(will_create)
    group_creates: set[str] = set()
    for step in plans:
        _validate_step(step, catalog, set(snapshot))
        _track_overwrite(step, group_creates)
    # Tables created by any branch become visible after the group.
    will_create |= group_creates


def _validate_step(step: StepPlan, catalog: TableDiscovery, will_create: set[str]) -> None:
    _check_sources(step, catalog, will_create)
    _check_target(step, catalog, will_create)
    _track_overwrite(step, will_create)


def _check_sources(step: StepPlan, catalog: TableDiscovery, will_create: set[str]) -> None:
    for binding in step.source_bindings:
        spec = binding.spec
        if (
            isinstance(spec, TableSourceSpec)
            and spec.table_ref.ref not in will_create
            and not catalog.exists(spec.table_ref)
        ):
            raise ETLCompilationError.unknown_source_table(
                step.step_type, binding.alias, spec.table_ref.ref
            )


def _check_target(step: StepPlan, catalog: TableDiscovery, will_create: set[str]) -> None:
    raw_spec = step.target_binding.spec
    if not _is_table_target_spec(raw_spec):
        return
    spec = raw_spec
    table_ref = cast(TableRef, spec.table_ref)
    if (
        _enum_value(spec.schema_mode) != _SCHEMA_MODE_OVERWRITE
        and table_ref.ref not in will_create
        and not catalog.exists(table_ref)
    ):
        raise ETLCompilationError.unknown_target_table(step.step_type, table_ref.ref)


def _track_overwrite(step: StepPlan, will_create: set[str]) -> None:
    raw_spec = step.target_binding.spec
    if not _is_table_target_spec(raw_spec):
        return
    spec = raw_spec
    if _enum_value(spec.schema_mode) == _SCHEMA_MODE_OVERWRITE:
        will_create.add(spec.table_ref.ref)


def _is_table_target_spec(spec: object) -> TypeGuard[_TableTargetSpecLike]:
    table_ref = getattr(spec, "table_ref", None)
    schema_mode = getattr(spec, "schema_mode", None)
    if table_ref is None or schema_mode is None:
        return False
    return isinstance(getattr(table_ref, "ref", None), str)


def _enum_value(value: object) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, Enum):
        raw = value.value
        if isinstance(raw, str):
            return raw
    return None
