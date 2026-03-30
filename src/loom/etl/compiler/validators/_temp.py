"""Compile-time FromTemp forward-reference and uniqueness validation."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from loom.etl.compiler._errors import ETLCompilationError
from loom.etl.compiler._plan import (
    PipelinePlan,
    ProcessPlan,
    StepPlan,
    visit_pipeline_nodes,
    visit_process_nodes,
)
from loom.etl.io._source import SourceKind
from loom.etl.io.target._temp import TempFanInSpec, TempSpec

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
    visit_pipeline_nodes(
        plan.nodes,
        lambda proc: _walk_process(proc, seen),
        on_parallel_group=lambda plans: _walk_parallel_process_group(plans, seen),
    )


# ---------------------------------------------------------------------------
# Tree walkers
# ---------------------------------------------------------------------------


def _walk_process(plan: ProcessPlan, seen: dict[str, bool]) -> None:
    visit_process_nodes(
        plan.nodes,
        lambda step: _validate_step(step, seen),
        on_parallel_group=lambda plans: _walk_parallel_step_group(plans, seen),
    )


def _walk_parallel_process_group(plans: tuple[ProcessPlan, ...], seen: dict[str, bool]) -> None:
    snapshot = dict(seen)
    group_seen: dict[str, bool] = {}
    for proc in plans:
        proc_seen = dict(snapshot)
        _walk_process(proc, proc_seen)
        group_seen.update({k: v for k, v in proc_seen.items() if k not in snapshot})
    seen.update(group_seen)


def _walk_parallel_step_group(plans: tuple[StepPlan, ...], seen: dict[str, bool]) -> None:
    snapshot = dict(seen)
    group_seen: dict[str, bool] = {}
    for step in plans:
        _check_sources(step, snapshot)
        _register_step_target(step, group_seen)
    seen.update(group_seen)


def _validate_step(step: StepPlan, seen: dict[str, bool]) -> None:
    _check_sources(step, seen)
    _register_step_target(step, seen)


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
    spec = step.target_binding.spec
    if not isinstance(spec, (TempSpec, TempFanInSpec)):
        return
    name = spec.temp_name
    is_fan_in = isinstance(spec, TempFanInSpec)
    existing = seen.get(name)
    if existing is None:
        seen[name] = is_fan_in
        return
    _CONFLICT[(existing, is_fan_in)](step.step_type, name)
