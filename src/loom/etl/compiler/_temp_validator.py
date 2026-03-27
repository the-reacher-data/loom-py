"""Compile-time FromTemp forward-reference and uniqueness validation."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

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

# ---------------------------------------------------------------------------
# Duplicate-name conflict dispatch
# ---------------------------------------------------------------------------
# Key: (existing_is_append, new_is_append)
# Value: handler called when the same temp name appears a second time.
# Using named functions keeps the map readable and avoids lambda-raise hacks.


def _allow(*_: Any) -> None:
    pass


def _raise_duplicate(step: type, name: str) -> None:
    raise ETLCompilationError.duplicate_temp_name(step, name)


def _raise_mix(step: type, name: str) -> None:
    raise ETLCompilationError.invalid_temp_append_mix(step, name)


_CONFLICT: dict[tuple[bool, bool], Callable[[type, str], None]] = {
    (True, True): _allow,  # fan-in: both writers opted in — OK
    (False, False): _raise_duplicate,  # strict duplicate — blocked
    (True, False): _raise_mix,  # mixed modes — ambiguous
    (False, True): _raise_mix,  # mixed modes — ambiguous
}


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def validate_plan_temps(plan: PipelinePlan) -> None:
    """Walk the plan validating temp forward-references and name uniqueness.

    Two invariants are enforced:

    * Every :class:`~loom.etl.FromTemp` must have a prior
      :class:`~loom.etl.IntoTemp` for the same name.
    * Two steps writing to the same name must both declare ``append=True``
      (fan-in).  A duplicate name without ``append=True`` is a compile error;
      mixing ``append=True`` and ``append=False`` is also an error.

    Mirrors the will_create logic used for catalog validation: parallel
    groups share the pre-group snapshot and merge their produces after.

    Args:
        plan: Fully compiled pipeline plan.

    Raises:
        ETLCompilationError: When a FromTemp references a name not yet
                             produced, a name is duplicated without
                             ``append=True``, or append modes are mixed.
    """
    seen: dict[str, bool] = {}  # name → is_append
    for node in plan.nodes:
        _walk_pipeline_node(node, seen)


# ---------------------------------------------------------------------------
# Tree walkers
# ---------------------------------------------------------------------------


def _walk_pipeline_node(node: PipelineProcessNode, seen: dict[str, bool]) -> None:
    match node:
        case ProcessPlan():
            _walk_process(node, seen)
        case ParallelProcessGroup(plans=plans):
            snapshot = dict(seen)
            group_seen: dict[str, bool] = {}
            for proc in plans:
                proc_seen = dict(snapshot)
                _walk_process(proc, proc_seen)
                group_seen.update({k: v for k, v in proc_seen.items() if k not in snapshot})
            seen.update(group_seen)


def _walk_process(plan: ProcessPlan, seen: dict[str, bool]) -> None:
    for node in plan.nodes:
        _walk_process_node(node, seen)


def _walk_process_node(node: ProcessStepNode, seen: dict[str, bool]) -> None:
    match node:
        case StepPlan():
            _check_sources(node, seen)
            _register_step_target(node, seen)
        case ParallelStepGroup(plans=plans):
            snapshot = dict(seen)
            group_seen: dict[str, bool] = {}
            for step in plans:
                _check_sources(step, snapshot)
                _register_step_target(step, group_seen)
            seen.update(group_seen)


# ---------------------------------------------------------------------------
# Step-level checks
# ---------------------------------------------------------------------------


def _check_sources(step: StepPlan, seen: dict[str, bool]) -> None:
    for binding in step.source_bindings:
        spec = binding.spec
        if spec.kind is SourceKind.TEMP and spec.temp_name not in seen:
            temp_name = spec.temp_name or ""
            raise ETLCompilationError.temp_not_produced(step.step_type, binding.alias, temp_name)


def _register_step_target(step: StepPlan, seen: dict[str, bool]) -> None:
    """Record the temp name produced by *step*, enforcing uniqueness rules."""
    name = step.target_binding.spec.temp_name
    if name is None:
        return
    append = step.target_binding.spec.temp_append
    existing = seen.get(name)
    if existing is None:
        seen[name] = append
        return
    _CONFLICT[(existing, append)](step.step_type, name)
