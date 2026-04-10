"""Compile-time validators for ETL plans.

This module consolidates all compiler validation logic:

* Catalog existence checks.
* Temp forward-reference and conflict checks.
* Step structural and ParamExpr checks.
* UPSERT target constraints.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol, TypeGuard, cast

import msgspec

from loom.etl.compiler._errors import ETLCompilationError
from loom.etl.compiler._plan import (
    PipelinePlan,
    ProcessPlan,
    SourceBinding,
    StepPlan,
    TargetBinding,
    visit_pipeline_nodes,
    visit_process_nodes,
)
from loom.etl.declarative.expr._params import ParamExpr
from loom.etl.declarative.expr._predicate import PredicateNode
from loom.etl.declarative.source._specs import TableSourceSpec, TempSourceSpec
from loom.etl.declarative.target import TargetSpec
from loom.etl.runtime.contracts import TableDiscovery

_SCHEMA_MODE_OVERWRITE = "overwrite"


class _TableRefLike(Protocol):
    ref: str


class _TableTargetSpecLike(Protocol):
    table_ref: _TableRefLike
    schema_mode: object


class _TempTargetSpecLike(Protocol):
    temp_name: str
    temp_scope: object


class _BinaryPredicateLike(Protocol):
    left: Any
    right: Any


class _InPredicateLike(Protocol):
    ref: Any
    values: Any


class _UnaryPredicateLike(Protocol):
    operand: Any


class _ReplaceWhereSpecLike(Protocol):
    replace_predicate: PredicateNode


class _UpsertSpecLike(Protocol):
    upsert_keys: tuple[str, ...]
    upsert_exclude: tuple[str, ...]
    upsert_include: tuple[str, ...]


class _PredicateShape(Enum):
    BINARY = "binary"
    IN = "in"
    UNARY = "unary"
    UNKNOWN = "unknown"


class _PredicateField(Enum):
    LEFT = "left"
    RIGHT = "right"
    REF = "ref"
    VALUES = "values"
    OPERAND = "operand"


@dataclass(frozen=True)
class StepCompilationContext:
    """Carries all resolved step data needed by per-step validators.

    Args:
        step_type: The concrete ``ETLStep`` subclass being compiled.
        params_type: The ``ParamsT`` generic argument.
        source_bindings: Resolved source alias → spec bindings.
        target_binding: Resolved target binding.
    """

    step_type: type[Any]
    params_type: type[Any]
    source_bindings: tuple[SourceBinding, ...]
    target_binding: TargetBinding


def validate_step(ctx: StepCompilationContext) -> None:
    """Run all per-step compile-time validators against *ctx*."""
    validate_execute_signature(ctx.step_type, ctx.params_type, ctx.source_bindings)
    validate_upsert_spec(ctx.step_type, ctx.target_binding.spec)
    validate_param_exprs(ctx.step_type, ctx.params_type, ctx.source_bindings, ctx.target_binding)


# ---------------------------------------------------------------------------
# Structural validation
# ---------------------------------------------------------------------------


def validate_execute_signature(
    step_type: type[Any],
    params_type: type[Any],
    source_bindings: tuple[SourceBinding, ...],
) -> None:
    """Validate that execute() has the correct params arg and matching keyword frames."""
    sig = inspect.signature(step_type.execute)
    params = list(sig.parameters.values())
    _validate_params_arg(step_type, params, params_type)
    if _is_sql_step_type(step_type):
        return
    kw_only = _collect_kw_only_frames(params)
    source_aliases = {b.alias for b in source_bindings}
    _check_missing_frames(step_type, source_aliases, kw_only)
    _check_extra_frames(step_type, source_aliases, kw_only)


def validate_params_compat(
    component_type: type[Any],
    component_params: type[Any],
    context_params: type[Any],
) -> None:
    """Raise if component_params requires fields absent from context_params."""
    if component_params is context_params:
        return
    if issubclass(context_params, component_params):
        return
    component_fields = {f.name for f in msgspec.structs.fields(component_params)}
    context_fields = {f.name for f in msgspec.structs.fields(context_params)}
    missing = component_fields - context_fields
    if missing:
        raise ETLCompilationError.missing_params_fields(
            component_type, frozenset(missing), context_params
        )


def _validate_params_arg(
    step_type: type[Any],
    params: list[inspect.Parameter],
    params_type: type[Any],
) -> None:
    positional_kinds = {
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.POSITIONAL_ONLY,
    }
    positional = [p for p in params if p.kind in positional_kinds and p.name != "self"]
    if not positional:
        raise ETLCompilationError.invalid_params_type(step_type, params_type)
    first = positional[0]
    if first.name != "params":
        raise ETLCompilationError.invalid_params_name(step_type, first.name)


def _collect_kw_only_frames(params: list[inspect.Parameter]) -> dict[str, inspect.Parameter]:
    return {p.name: p for p in params if p.kind is inspect.Parameter.KEYWORD_ONLY}


def _is_sql_step_type(step_type: type[Any]) -> bool:
    """Return ``True`` when step type follows StepSQL marker contract."""
    return bool(getattr(step_type, "_loom_sql_step", False))


def _check_missing_frames(
    step_type: type[Any],
    source_aliases: set[str],
    kw_only: dict[str, inspect.Parameter],
) -> None:
    missing = source_aliases - set(kw_only)
    if missing:
        raise ETLCompilationError.missing_source_params(step_type, frozenset(missing))


def _check_extra_frames(
    step_type: type[Any],
    source_aliases: set[str],
    kw_only: dict[str, inspect.Parameter],
) -> None:
    extra = set(kw_only) - source_aliases
    if extra:
        raise ETLCompilationError.extra_source_params(step_type, frozenset(extra))


# ---------------------------------------------------------------------------
# ParamExpr validation
# ---------------------------------------------------------------------------


def validate_param_exprs(
    step_type: type[Any],
    params_type: type[Any],
    source_bindings: tuple[SourceBinding, ...],
    target_binding: TargetBinding,
) -> None:
    """Raise when any ParamExpr references an undeclared params field."""
    known = _known_fields(params_type)
    if known is None:
        return

    exprs: list[ParamExpr] = []
    for binding in source_bindings:
        for pred in getattr(binding.spec, "predicates", ()):
            _collect_exprs(pred, exprs)

    if _is_replace_where_like(target_binding.spec):
        target_spec = cast(_ReplaceWhereSpecLike, target_binding.spec)
        _collect_exprs(target_spec.replace_predicate, exprs)

    for expr in exprs:
        field_name = expr.path[0]
        if field_name not in known:
            raise ETLCompilationError.unknown_param_field(step_type, field_name, params_type)


def _known_fields(params_type: type[Any]) -> frozenset[str] | None:
    try:
        if not (isinstance(params_type, type) and issubclass(params_type, msgspec.Struct)):
            return None
        return frozenset(f.name for f in msgspec.structs.fields(params_type))
    except Exception:
        return None


def _collect_exprs(node: PredicateNode, out: list[ParamExpr]) -> None:
    shape = _predicate_shape(node)
    if shape is _PredicateShape.BINARY:
        binary = cast(_BinaryPredicateLike, node)
        left = binary.left
        right = binary.right
        if _is_predicate_node_like(left) or _is_predicate_node_like(right):
            _collect_exprs(left, out)
            _collect_exprs(right, out)
            return
        _collect_value(left, out)
        _collect_value(right, out)
        return
    if shape is _PredicateShape.IN:
        in_pred = cast(_InPredicateLike, node)
        _collect_value(in_pred.ref, out)
        _collect_value(in_pred.values, out)
        return
    if shape is _PredicateShape.UNARY:
        unary = cast(_UnaryPredicateLike, node)
        _collect_exprs(unary.operand, out)


def _collect_value(value: Any, out: list[ParamExpr]) -> None:
    if _is_param_expr(value):
        out.append(value)


def _is_param_expr(value: Any) -> bool:
    if isinstance(value, ParamExpr):
        return True
    path = getattr(value, "path", None)
    return isinstance(path, tuple) and all(isinstance(part, str) for part in path)


def _predicate_shape(node: Any) -> _PredicateShape:
    fields = getattr(type(node), "__dataclass_fields__", None)
    if not isinstance(fields, dict):
        return _PredicateShape.UNKNOWN
    if _has_fields(fields, _PredicateField.LEFT, _PredicateField.RIGHT):
        return _PredicateShape.BINARY
    if _has_fields(fields, _PredicateField.REF, _PredicateField.VALUES):
        return _PredicateShape.IN
    if _has_fields(fields, _PredicateField.OPERAND):
        return _PredicateShape.UNARY
    return _PredicateShape.UNKNOWN


def _is_predicate_node_like(value: Any) -> bool:
    return _predicate_shape(value) is not _PredicateShape.UNKNOWN


def _has_fields(fields: dict[str, Any], *expected: _PredicateField) -> bool:
    if len(fields) != len(expected):
        return False
    return all(field.value in fields for field in expected)


def _is_replace_where_like(spec: object) -> bool:
    return hasattr(spec, "replace_predicate") and hasattr(spec, "table_ref")


# ---------------------------------------------------------------------------
# UPSERT validation
# ---------------------------------------------------------------------------


def validate_upsert_spec(step_type: type[Any], spec: TargetSpec) -> None:
    """Validate UPSERT-specific constraints at compile time."""
    if not _is_upsert_like(spec):
        return
    upsert_spec = cast(_UpsertSpecLike, spec)
    _check_upsert_keys_non_empty(step_type, upsert_spec)
    _check_upsert_exclude_include_exclusive(step_type, upsert_spec)
    _check_upsert_exclude_keys_disjoint(step_type, upsert_spec)


def _check_upsert_keys_non_empty(step_type: type[Any], spec: _UpsertSpecLike) -> None:
    if not spec.upsert_keys:
        raise ETLCompilationError.upsert_no_keys(step_type)


def _check_upsert_exclude_include_exclusive(step_type: type[Any], spec: _UpsertSpecLike) -> None:
    if spec.upsert_exclude and spec.upsert_include:
        raise ETLCompilationError.upsert_exclude_include_conflict(step_type)


def _check_upsert_exclude_keys_disjoint(step_type: type[Any], spec: _UpsertSpecLike) -> None:
    overlap = frozenset(spec.upsert_exclude) & frozenset(spec.upsert_keys)
    if overlap:
        raise ETLCompilationError.upsert_key_in_exclude(step_type, overlap)


def _is_upsert_like(spec: object) -> bool:
    keys = getattr(spec, "upsert_keys", None)
    exclude = getattr(spec, "upsert_exclude", None)
    include = getattr(spec, "upsert_include", None)
    return isinstance(keys, tuple) and isinstance(exclude, tuple) and isinstance(include, tuple)


# ---------------------------------------------------------------------------
# Catalog validation
# ---------------------------------------------------------------------------


def validate_step_catalog(plan: StepPlan, catalog: TableDiscovery) -> None:
    """Validate a single step's sources and target against the catalog."""
    _validate_step_catalog(plan, catalog, set())


def validate_process_catalog(plan: ProcessPlan, catalog: TableDiscovery) -> None:
    """Validate all steps in a process against the catalog."""
    _walk_process_catalog(plan, catalog, set())


def validate_plan_catalog(plan: PipelinePlan, catalog: TableDiscovery) -> None:
    """Walk the plan validating all table references against the catalog."""
    will_create: set[str] = set()
    visit_pipeline_nodes(
        plan.nodes,
        lambda proc: _walk_process_catalog(proc, catalog, will_create),
        on_parallel_group=lambda plans: _walk_parallel_process_group(plans, catalog, will_create),
    )


def _walk_process_catalog(
    plan: ProcessPlan,
    catalog: TableDiscovery,
    will_create: set[str],
) -> None:
    visit_process_nodes(
        plan.nodes,
        lambda step: _validate_step_catalog(step, catalog, will_create),
        on_parallel_group=lambda plans: _walk_parallel_step_group(plans, catalog, will_create),
    )


def _walk_parallel_process_group(
    plans: tuple[ProcessPlan, ...],
    catalog: TableDiscovery,
    will_create: set[str],
) -> None:
    snapshot = frozenset(will_create)
    group_creates: set[str] = set()
    for proc in plans:
        proc_creates = set(snapshot)
        _walk_process_catalog(proc, catalog, proc_creates)
        group_creates |= proc_creates - snapshot
    will_create |= group_creates


def _walk_parallel_step_group(
    plans: tuple[StepPlan, ...],
    catalog: TableDiscovery,
    will_create: set[str],
) -> None:
    snapshot = frozenset(will_create)
    group_creates: set[str] = set()
    for step in plans:
        _validate_step_catalog(step, catalog, set(snapshot))
        _track_overwrite(step, group_creates)
    will_create |= group_creates


def _validate_step_catalog(
    step: StepPlan,
    catalog: TableDiscovery,
    will_create: set[str],
) -> None:
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
    table_ref = cast(_TableRefLike, spec.table_ref)
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


# ---------------------------------------------------------------------------
# Temp validation
# ---------------------------------------------------------------------------


def _allow(*_: Any) -> None:
    return None


def _raise_duplicate(step: type, name: str) -> None:
    raise ETLCompilationError.duplicate_temp_name(step, name)


def _raise_mix(step: type, name: str) -> None:
    raise ETLCompilationError.invalid_temp_append_mix(step, name)


_CONFLICT: dict[tuple[bool, bool], Callable[[type, str], None]] = {
    (True, True): _allow,
    (False, False): _raise_duplicate,
    (True, False): _raise_mix,
    (False, True): _raise_mix,
}


def validate_plan_temps(plan: PipelinePlan) -> None:
    """Walk the plan validating temp forward-references and name uniqueness."""
    seen: dict[str, bool] = {}
    visit_pipeline_nodes(
        plan.nodes,
        lambda proc: _walk_process_temps(proc, seen),
        on_parallel_group=lambda plans: _walk_parallel_process_group_temps(plans, seen),
    )


def _walk_process_temps(plan: ProcessPlan, seen: dict[str, bool]) -> None:
    visit_process_nodes(
        plan.nodes,
        lambda step: _validate_step_temps(step, seen),
        on_parallel_group=lambda plans: _walk_parallel_step_group_temps(plans, seen),
    )


def _walk_parallel_process_group_temps(
    plans: tuple[ProcessPlan, ...], seen: dict[str, bool]
) -> None:
    snapshot = dict(seen)
    group_seen: dict[str, bool] = {}
    for proc in plans:
        proc_seen = dict(snapshot)
        _walk_process_temps(proc, proc_seen)
        group_seen.update({k: v for k, v in proc_seen.items() if k not in snapshot})
    seen.update(group_seen)


def _walk_parallel_step_group_temps(plans: tuple[StepPlan, ...], seen: dict[str, bool]) -> None:
    snapshot = dict(seen)
    group_seen: dict[str, bool] = {}
    for step in plans:
        _check_temp_sources(step, snapshot)
        _register_step_target(step, group_seen)
    seen.update(group_seen)


def _validate_step_temps(step: StepPlan, seen: dict[str, bool]) -> None:
    _check_temp_sources(step, seen)
    _register_step_target(step, seen)


def _check_temp_sources(step: StepPlan, seen: dict[str, bool]) -> None:
    for binding in step.source_bindings:
        spec = binding.spec
        if isinstance(spec, TempSourceSpec) and spec.temp_name not in seen:
            raise ETLCompilationError.temp_not_produced(
                step.step_type, binding.alias, spec.temp_name
            )


def _register_step_target(step: StepPlan, seen: dict[str, bool]) -> None:
    raw_spec = step.target_binding.spec
    if not _is_temp_target_spec(raw_spec):
        return
    spec = raw_spec
    target_kind = _temp_target_kind(spec)
    if target_kind is None:
        return
    name = spec.temp_name
    is_fan_in = target_kind == "fan_in"
    existing = seen.get(name)
    if existing is None:
        seen[name] = is_fan_in
        return
    _CONFLICT[(existing, is_fan_in)](step.step_type, name)


def _temp_target_kind(spec: object) -> str | None:
    if not _is_temp_target_spec(spec):
        return None
    kind = type(spec).__name__
    if kind == "TempSpec":
        return "strict"
    if kind == "TempFanInSpec":
        return "fan_in"
    return None


def _is_temp_target_spec(spec: object) -> TypeGuard[_TempTargetSpecLike]:
    return isinstance(getattr(spec, "temp_name", None), str) and hasattr(spec, "temp_scope")


__all__ = [
    "StepCompilationContext",
    "validate_execute_signature",
    "validate_param_exprs",
    "validate_params_compat",
    "validate_plan_catalog",
    "validate_plan_temps",
    "validate_process_catalog",
    "validate_step",
    "validate_step_catalog",
    "validate_upsert_spec",
]
